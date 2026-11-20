package com.github.olehbohatyi.mep.it

import zio.*
import zio.kafka.consumer.Consumer
import zio.redis.Redis
import zio.test.*

import javax.sql.DataSource

import com.github.olehbohatyi.mep.broker.{ ProducerService, TopicBootstrap }
import com.github.olehbohatyi.mep.caches.*
import com.github.olehbohatyi.mep.config.*
import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.gate.{ EventRequest, IngestionResult, IngestionService }
import com.github.olehbohatyi.mep.geo.RegionResolver
import com.github.olehbohatyi.mep.it.Containers.*
import com.github.olehbohatyi.mep.pipe.*
import com.github.olehbohatyi.mep.stores.*
import com.github.olehbohatyi.mep.testkit.{ TestLifecycle, TestTelemetry }

// What the delivery guarantees in the README promise when Postgres, Redis or the broker stop answering. Outages are
// `docker pause`: connections hang rather than being refused, the harder case for timeouts.
object OutageSpec extends ZIOSpecDefault:

  private val request = EventRequest(EventType.Hazard, Severity.High, GeoPoint(50.45, 30.52), Some("outage"), None)

  private def eventually[R, A](effect: RIO[R, Option[A]]): RIO[R, A] =
    effect.someOrFail(RuntimeException("not yet")).retry(Schedule.spaced(250.millis) && Schedule.recurs(240))

  // The pipe as its app wires it, with its own consumer: building it again is a restart of the process
  private val pipeLayer: RLayer[KafkaConfig & RecordHandler, StreamProcessor] =
    ZLayer.makeSome[KafkaConfig & RecordHandler, StreamProcessor](
      StreamProcessor.consumerSettings,
      Consumer.live,
      StreamProcessor.live,
    )

  // Runs the pipe until it stops by itself, or for at most `limit`; None when it was still running
  private def runPipe(limit: Duration): RIO[KafkaConfig & RecordHandler, Option[Exit[Throwable, Unit]]] =
    ZIO.scoped(pipeLayer.build.flatMap(_.get.run.exit.timeout(limit)))

  // Runs the pipe in the background until the enclosing scope closes
  private val backgroundPipe: RIO[Scope & KafkaConfig & RecordHandler, Unit] =
    pipeLayer.build.flatMap(_.get.run.forkScoped).unit

  // Rows for this event id; more than one would be a duplicate
  private def rows(id: java.util.UUID): RIO[DataSource, Long] =
    ZIO.serviceWithZIO[DataSource](ds =>
      ZIO.attemptBlocking {
        val c = ds.getConnection
        try
          val st = c.prepareStatement("select count(*) from map_events where id = ?")
          st.setObject(1, id)
          val rs = st.executeQuery()
          rs.next()
          rs.getLong(1)
        finally c.close()
      }
    )

  // A pipe that cannot make progress must stop within this and be restarted, not sit on its partitions
  private val StopsWithin = 90.seconds

  // How long a client may wait for the gate's answer while Kafka is down
  private val AnswersWithin = 30.seconds

  def spec = (suite("Outages")(
    test("a pipe that cannot reach Postgres stops without committing; after a restart the event is stored once") {
      for
        gate     <- ZIO.service[IngestionService]
        outage   <- ZIO.service[PostgresOutage].map(_.outage)
        accepted <- gate.ingest(request, Some("pg-outage"))
        id        = IngestionService.eventId("pg-outage")
        first    <- outage.during(runPipe(StopsWithin))
        stored   <- ZIO.scoped(backgroundPipe *> eventually(ZIO.serviceWithZIO[EventRepo](_.findById(id))))
        count    <- rows(id)
      yield assertTrue(
        accepted == IngestionResult.Accepted(id),
        // It stopped with an error instead of hanging
        first.exists(_.isFailure),
        stored.id == id,
        count == 1L,
      )
    },
    test("a query in flight when Postgres stops answering fails instead of hanging") {
      for
        outage <- ZIO.service[PostgresOutage].map(_.outage)
        query  <- ZIO
                    .serviceWithZIO[DataSource](ds =>
                      ZIO.attemptBlocking {
                        val c = ds.getConnection
                        try c.createStatement().execute("select pg_sleep(3)")
                        finally c.close()
                      }
                    )
                    .fork
        // None: still blocked after StopsWithin
        exit   <- outage.during(ZIO.sleep(1.second) *> query.await.timeout(StopsWithin))
      yield assertTrue(exit.exists(_.isFailure))
    },
    test("a pipe that cannot reach Redis stops; after a restart the event is stored once") {
      for
        gate     <- ZIO.service[IngestionService]
        outage   <- ZIO.service[RedisOutage].map(_.outage)
        // Without a key the gate does not need Redis
        accepted <- gate.ingest(request, None)
        id       <- accepted match
                      case IngestionResult.Accepted(id) => ZIO.succeed(id)
                      case other                        => ZIO.fail(RuntimeException(s"not accepted: $other"))
        first    <- outage.during(runPipe(StopsWithin))
        stored   <- ZIO.scoped(backgroundPipe *> eventually(ZIO.serviceWithZIO[EventRepo](_.findById(id))))
        count    <- rows(id)
      yield assertTrue(first.exists(_.isFailure), stored.id == id, count == 1L)
    },
    test("while Kafka is down the gate answers in time and releases the key, so the client's retry is accepted") {
      for
        gate   <- ZIO.service[IngestionService]
        outage <- ZIO.service[KafkaOutage].map(_.outage)
        during <- outage.during(gate.ingest(request, Some("kafka-outage")).timeout(AnswersWithin).exit)
        after  <- gate.ingest(request, Some("kafka-outage"))
      yield assertTrue(
        // An error, not Success(None): the client was not left waiting
        during.isFailure,
        after == IngestionResult.Accepted(IngestionService.eventId("kafka-outage")),
      )
    },
    // A client that gives up waiting and retries must not be told the event was published while it is not yet
    test("a retry while the first attempt is still publishing is not answered as a duplicate") {
      for
        gate   <- ZIO.service[IngestionService]
        outage <- ZIO.service[KafkaOutage].map(_.outage)
        retry  <- outage.during(
                    for
                      first <- gate.ingest(request, Some("in-flight")).fork
                      _     <- ZIO.sleep(2.seconds)
                      retry <- gate.ingest(request, Some("in-flight")).timeout(AnswersWithin).exit
                      _     <- first.interrupt
                    yield retry
                  )
      yield assertTrue(!retry.toEither.toOption.flatten.exists(_.isInstanceOf[IngestionResult.Duplicate]))
    },
  ) @@ TestAspect.beforeAll(
    Live.live(Migrations.run *> TopicBootstrap.run).orDie
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock @@ TestAspect.timeout(10.minutes))
    .provideShared(
      TestTelemetry.recording,
      Containers.kafkaWithOutage,
      Containers.postgresWithOutage,
      Containers.redisWithOutage,
      ZLayer.service[RedisConfig] >>> RedisService.live,
      DataSourceLayer.live,
      EventRepo.live,
      ProducerService.live,
      GeoCache.live,
      RegionRepo.live,
      RegionResolver.live,
      ZLayer.makeSome[Redis & ProducerService & KafkaConfig, IngestionService](
        IdempotencyCache.live(IngestionService.Namespace),
        IngestionService.live,
      ),
      ZLayer.makeSome[Redis, DeduplicationService](
        IdempotencyCache.live(DeduplicationService.Namespace),
        DeduplicationService.live,
      ),
      EnrichmentService.live,
      EscalationService.live,
      EventHandler.live,
      ZLayer.succeed(TestLifecycle.config),
      LifecycleService.live,
      RecordHandler.live,
    )
