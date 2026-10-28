package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.test.*

import com.github.olehbohatyi.mep.config.KafkaConfig
import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.geo.RegionResolver
import com.github.olehbohatyi.mep.testkit.*

object EventHandlerSpec extends ZIOSpecDefault:

  val kafka = TestKafka.config()

  // Escalation without Redis: route by type, never escalate
  val routingOnly: URLayer[KafkaConfig, EscalationService] = ZLayer.fromFunction((cfg: KafkaConfig) =>
    new EscalationService:
      def topicFor(event: MapEvent): String         =
        event.eventType match
          case EventType.Hazard  => cfg.topics.hazards
          case EventType.Sign    => cfg.topics.signs
          case EventType.Traffic => cfg.topics.traffic
      def escalate(event: MapEvent): Task[MapEvent] = ZIO.succeed(event)
  )

  def spec = suite("EventHandler")(
    test("enriches, stores and publishes to enriched + typed topic") {
      val event = Samples.event(eventType = EventType.Traffic)
      for
        handler  <- ZIO.service[EventHandler]
        repo     <- ZIO.service[InMemoryEventRepo]
        producer <- ZIO.service[RecordingProducer]
        outcome  <- handler.handle(event)
        stored   <- repo.findById(event.id)
        sent     <- producer.all
      yield assertTrue(
        outcome == Outcome.Processed("traffic"),
        stored.exists(e => e.status == EventStatus.Enriched && e.region.contains("zone_50_30")),
        sent.map(_._1) == Chunk("enriched", "traffic"),
        sent.forall(_._2.status == EventStatus.Enriched),
      )
    },
    test("enriches with the named region when a region polygon contains the point") {
      val event = Samples.event(location = Samples.Lviv)
      for
        handler <- ZIO.service[EventHandler]
        repo    <- ZIO.service[InMemoryEventRepo]
        _       <- handler.handle(event)
        stored  <- repo.findById(event.id)
      yield assertTrue(stored.exists(_.region.contains("Lviv Oblast")))
    },
    test("skips events already processed") {
      val event = Samples.event()
      for
        handler  <- ZIO.service[EventHandler]
        producer <- ZIO.service[RecordingProducer]
        _        <- handler.handle(event)
        again    <- handler.handle(event)
        sent     <- producer.all
      yield assertTrue(again == Outcome.Duplicate, sent.size == 2)
    },
    test("marks the event processed under the pipe namespace only after publishing") {
      val event = Samples.event()
      for
        handler  <- ZIO.service[EventHandler]
        producer <- ZIO.service[RecordingProducer]
        cache    <- ZIO.service[InMemoryIdempotencyCache]
        _        <- producer.failNext(1)
        failed   <- handler.handle(event).exit
        before   <- cache.all
        retried  <- handler.handle(event)
        after    <- cache.all
      yield assertTrue(
        failed.isFailure,
        before.isEmpty,
        retried == Outcome.Processed("hazards"),
        after == Set(event.id.toString),
      )
    },
    test("a redelivered event that is already stored is still published") {
      val event = Samples.event()
      for
        handler  <- ZIO.service[EventHandler]
        repo     <- ZIO.service[InMemoryEventRepo]
        producer <- ZIO.service[RecordingProducer]
        _        <- repo.insert(event.copy(status = EventStatus.Enriched))
        outcome  <- handler.handle(event)
        sent     <- producer.all
        rows     <- repo.all
      yield assertTrue(outcome == Outcome.Processed("hazards"), sent.size == 2, rows.size == 1)
    },
  ).provide(
    TestTelemetry.tracing,
    ZLayer.succeed(kafka),
    InMemoryEventRepo.layer,
    RecordingProducer.layer,
    InMemoryIdempotencyCache.layer,
    InMemoryGeoCache.layer,
    InMemoryRegionRepo.layer(Samples.LvivOblast),
    RegionResolver.live,
    DeduplicationService.live,
    EnrichmentService.live,
    routingOnly,
    EventHandler.live,
  )
