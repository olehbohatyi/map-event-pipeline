package com.github.olehbohatyi.mep.it

import zio.*
import zio.kafka.consumer.{ Consumer, ConsumerSettings, Subscription }
import zio.kafka.producer.{ Producer, ProducerSettings }
import zio.kafka.serde.Serde
import zio.redis.Redis
import zio.test.*

import com.github.olehbohatyi.mep.broker.{ EventCodec, ProducerService, TopicBootstrap }
import com.github.olehbohatyi.mep.caches.*
import com.github.olehbohatyi.mep.config.*
import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.gate.{ EventRequest, IngestionResult, IngestionService }
import com.github.olehbohatyi.mep.geo.RegionResolver
import com.github.olehbohatyi.mep.pipe.*
import com.github.olehbohatyi.mep.stores.*

// End to end: gate ingestion → Kafka raw → pipe → Postgres + typed topic
object PipelineSpec extends ZIOSpecDefault:

  private val request = EventRequest(EventType.Hazard, Severity.High, GeoPoint(50.45, 30.52), Some("flood"), None)

  private def eventually[R, A](effect: RIO[R, Option[A]]): RIO[R, A] =
    effect.someOrFail(RuntimeException("not yet")).retry(Schedule.spaced(250.millis) && Schedule.recurs(120))

  private def readTopic(cfg: KafkaConfig, topic: String, n: Int): Task[Chunk[MapEvent]] =
    ZIO.scoped {
      Consumer
        .make(
          ConsumerSettings(cfg.servers)
            .withGroupId(s"reader-$topic")
            .withOffsetRetrieval(Consumer.OffsetRetrieval.Auto(Consumer.AutoOffsetStrategy.Earliest))
        )
        .flatMap(_.plainStream(Subscription.topics(topic), Serde.string, Serde.string).take(n.toLong).runCollect)
        .map(_.flatMap(r => EventCodec.decode(r.value).toOption))
    }

  // Raw records with their headers, e.g. what landed on the dead-letter topic
  private def readRaw(cfg: KafkaConfig, topic: String, n: Int): Task[Chunk[(String, Map[String, String])]] =
    ZIO.scoped {
      Consumer
        .make(
          ConsumerSettings(cfg.servers)
            .withGroupId(s"reader-raw-$topic")
            .withOffsetRetrieval(Consumer.OffsetRetrieval.Auto(Consumer.AutoOffsetStrategy.Earliest))
        )
        .flatMap(_.plainStream(Subscription.topics(topic), Serde.string, Serde.string).take(n.toLong).runCollect)
        .map(_.map { r =>
          r.value -> r
            .record
            .headers
            .toArray
            .map(h => h.key -> String(h.value, java.nio.charset.StandardCharsets.UTF_8))
            .toMap
        })
    }

  def spec = (suite("Pipeline")(
    test("an ingested event is enriched, stored and routed; malformed records go to the DLQ") {
      for
        cfg       <- ZIO.service[KafkaConfig]
        processor <- ZIO.serviceWithZIO[StreamProcessor](_.run).fork
        // A poison record first: it must not block the partition
        _         <- ZIO.scoped(
                       Producer
                         .make(ProducerSettings(cfg.servers))
                         .flatMap(_.produce(cfg.topics.raw, "bad", "{oops", Serde.string, Serde.string))
                     )
        gate      <- ZIO.service[IngestionService]
        first     <- gate.ingest(request, Some("e2e-1"))
        retry     <- gate.ingest(request, Some("e2e-1"))
        id         = IngestionService.eventId("e2e-1")
        repo      <- ZIO.service[EventRepo]
        stored    <- eventually(repo.findById(id))
        hazards   <- readTopic(cfg, cfg.topics.hazards, 1).timeoutFail(RuntimeException("no hazard event"))(60.seconds)
        dlq       <- readRaw(cfg, cfg.topics.dlq, 1).timeoutFail(RuntimeException("no dead letter"))(60.seconds)
        _         <- processor.interrupt
      yield assertTrue(
        first == IngestionResult.Accepted(id),
        retry == IngestionResult.Duplicate(id),
        stored.status == EventStatus.Enriched,
        stored.region.contains("zone_50_30"),
        hazards.map(_.id) == Chunk(id),
        dlq.map(_._1) == Chunk("{oops"),
        dlq.head._2(RecordHandler.Headers.SourceTopic) == cfg.topics.raw,
        dlq.head._2(RecordHandler.Headers.Error).nonEmpty,
      )
    }
  ) @@ TestAspect.beforeAll(
    Live.live(Migrations.run *> TopicBootstrap.run).orDie
  ) @@ TestAspect.withLiveClock @@ TestAspect
    .timeout(5.minutes))
    .provideShared(
      Containers.kafka,
      Containers.postgres,
      Containers.redis >>> RedisService.live,
      DataSourceLayer.live,
      EventRepo.live,
      ProducerService.live,
      StreamProcessor.consumerSettings,
      Consumer.live,
      GeoCache.live,
      RegionResolver.live,
      // gate and pipe use separate keyspaces, each with its own cache instance
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
      RecordHandler.live,
      StreamProcessor.live,
    )
