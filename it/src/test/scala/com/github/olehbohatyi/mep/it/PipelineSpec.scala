package com.github.olehbohatyi.mep.it

import zio.*
import zio.kafka.consumer.{ Consumer, ConsumerSettings, Subscription }
import zio.kafka.producer.{ Producer, ProducerSettings }
import zio.kafka.serde.Serde
import zio.redis.Redis
import zio.telemetry.opentelemetry.tracing.Tracing
import zio.test.*

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import org.apache.kafka.clients.producer.ProducerRecord
import scala.jdk.CollectionConverters.*
import com.github.olehbohatyi.mep.broker.{ EventCodec, ProducerService, TopicBootstrap }
import com.github.olehbohatyi.mep.caches.*
import com.github.olehbohatyi.mep.config.*
import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.gate.{ EventRequest, IngestionResult, IngestionService }
import com.github.olehbohatyi.mep.geo.RegionResolver
import com.github.olehbohatyi.mep.pipe.*
import com.github.olehbohatyi.mep.stores.*
import com.github.olehbohatyi.mep.testkit.TestTelemetry

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

  // A record as it landed on a topic, e.g. the dead-letter topic
  final private case class Landed(key: Option[String], value: String, headers: Map[String, String])

  private def readRaw(cfg: KafkaConfig, topic: String, n: Int): Task[Chunk[Landed]] =
    ZIO.scoped {
      Consumer
        .make(
          ConsumerSettings(cfg.servers)
            .withGroupId(s"reader-raw-$topic")
            .withOffsetRetrieval(Consumer.OffsetRetrieval.Auto(Consumer.AutoOffsetStrategy.Earliest))
        )
        .flatMap(_.plainStream(Subscription.topics(topic), Serde.string, Serde.string).take(n.toLong).runCollect)
        .map(_.map { r =>
          val headers = r
            .record
            .headers
            .toArray
            .map(h => h.key -> String(h.value, java.nio.charset.StandardCharsets.UTF_8))
            .toMap
          Landed(Option(r.key), r.value, headers)
        })
    }

  // Committed offset per partition of `topic` for the pipe's group, and the end offsets
  private def progress(cfg: KafkaConfig, topic: String): Task[(Map[Int, Long], Map[Int, Long])] =
    ZIO.scoped {
      zio.kafka.admin.AdminClient.make(zio.kafka.admin.AdminClientSettings(cfg.servers)).flatMap { admin =>
        for
          committed <- admin.listConsumerGroupOffsets(cfg.consumerGroup)
          parts      = committed.keySet.filter(_.name == topic)
          ends      <- admin.listOffsets(parts.map(_ -> zio.kafka.admin.AdminClient.OffsetSpec.LatestSpec).toMap)
        yield (
          committed.collect { case (tp, o) if tp.name == topic => tp.partition -> o.offset },
          ends.map((tp, o) => tp.partition -> o.offset),
        )
      }
    }

  def spec = (suite("Pipeline")(
    test("an ingested event is enriched, stored and routed; malformed records go to the DLQ") {
      for
        cfg       <- ZIO.service[KafkaConfig]
        processor <- ZIO.serviceWithZIO[StreamProcessor](_.run).fork
        // Poison records first: they must not block the partition, with and without a key
        _         <- ZIO.scoped(
                       Producer.make(ProducerSettings(cfg.servers)).flatMap { producer =>
                         val noKey: String = null
                         producer.produce(cfg.topics.raw, "bad", "{oops", Serde.string, Serde.string) *>
                           producer.produce(
                             ProducerRecord(cfg.topics.raw, noKey, "{oops-keyless"),
                             Serde.string,
                             Serde.string,
                           )
                       }
                     )
        gate      <- ZIO.service[IngestionService]
        tracing   <- ZIO.service[Tracing]
        // Stands in for the gate's HTTP server span
        first     <- tracing.span("client request")(gate.ingest(request, Some("e2e-1")))
        retry     <- gate.ingest(request, Some("e2e-1"))
        id         = IngestionService.eventId("e2e-1")
        repo      <- ZIO.service[EventRepo]
        stored    <- eventually(repo.findById(id))
        hazards   <- readTopic(cfg, cfg.topics.hazards, 1).timeoutFail(RuntimeException("no hazard event"))(60.seconds)
        dlq       <- readRaw(cfg, cfg.topics.dlq, 2).timeoutFail(RuntimeException("no dead letter"))(60.seconds)
        // The pipe continues the trace from the raw record's headers
        spans     <- ZIO.service[InMemorySpanExporter]
        trace     <- eventually(ZIO.attempt {
                       val all  = spans.getFinishedSpanItems.asScala.toList
                       val same = all
                         .find(_.getName == "client request")
                         .map(root => all.filter(_.getTraceId == root.getTraceId).map(_.getName).toSet)
                       same.filter(names => names("raw process") && names("enriched publish"))
                     })
        _         <- processor.interrupt
      yield assertTrue(
        first == IngestionResult.Accepted(id),
        retry == IngestionResult.Duplicate(id),
        stored.status == EventStatus.Enriched,
        stored.region.contains("Kyiv City"),
        hazards.map(_.id) == Chunk(id),
        // Payload and key survive unchanged; a missing key stays missing
        dlq.map(r => r.value -> r.key).toMap == Map("{oops" -> Some("bad"), "{oops-keyless" -> None),
        dlq.forall(_.headers(RecordHandler.Headers.SourceTopic) == cfg.topics.raw),
        dlq.forall(_.headers(RecordHandler.Headers.Error).nonEmpty),
        Set("raw publish", "raw process", "enrich", "store", "enriched publish", "hazards publish").subsetOf(trace),
      )
    },
    // The pipe processes partitions in parallel: every event on every partition is handled and committed
    test("events on every partition are processed and their offsets committed") {
      for
        cfg       <- ZIO.service[KafkaConfig]
        processor <- ZIO.serviceWithZIO[StreamProcessor](_.run).fork
        gate      <- ZIO.service[IngestionService]
        results   <- ZIO.foreach(1 to 30)(i => gate.ingest(request, Some(s"spread-$i")))
        ids        = results.collect { case IngestionResult.Accepted(id) => id }
        repo      <- ZIO.service[EventRepo]
        _         <- eventually(ZIO.foreach(ids)(repo.findById).map(found => Option.when(found.forall(_.isDefined))(())))
        caughtUp  <- eventually(
                       progress(cfg, cfg.topics.raw).map((done, end) =>
                         Option.when(done.size == cfg.topicSettings.partitions && done == end)(done)
                       )
                     )
        _         <- processor.interrupt
      yield assertTrue(ids.size == 30, caughtUp.size == cfg.topicSettings.partitions, caughtUp.values.forall(_ > 0))
    },
  ) @@ TestAspect.beforeAll(
    Live.live(Migrations.run *> TopicBootstrap.run).orDie
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock @@ TestAspect
    .timeout(5.minutes))
    .provideShared(
      TestTelemetry.recording,
      Containers.kafka,
      Containers.postgres,
      Containers.redis >>> RedisService.live,
      DataSourceLayer.live,
      EventRepo.live,
      ProducerService.live,
      StreamProcessor.consumerSettings,
      Consumer.live,
      GeoCache.live,
      RegionRepo.live,
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
