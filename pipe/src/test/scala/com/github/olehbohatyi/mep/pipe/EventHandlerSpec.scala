package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.concurrent.CountdownLatch
import zio.test.*

import com.github.olehbohatyi.mep.broker.ProducerService
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

  // Each publish returns only once both have started: sent one after the other, the first never returns
  val meeting: ULayer[ProducerService] = ZLayer.fromZIO(CountdownLatch.make(2).map { latch =>
    new ProducerService:
      def publish(topic: String, event: MapEvent): Task[Unit]                                                     = latch.countDown *> latch.await
      def publishRaw(topic: String, key: Option[String], value: String, headers: Map[String, String]): Task[Unit] =
        ZIO.unit
  })

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
        sent.map(_._1).toSet == Set("enriched", "traffic"),
        sent.size == 2,
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
    test("a redelivered event that was closed since is not published again, so it stays off the live map") {
      val event = Samples.event()
      for
        handler  <- ZIO.service[EventHandler]
        repo     <- ZIO.service[InMemoryEventRepo]
        producer <- ZIO.service[RecordingProducer]
        _        <- repo.insert(event.copy(status = EventStatus.Resolved))
        outcome  <- handler.handle(event)
        sent     <- producer.all
        stored   <- repo.findById(event.id)
      yield assertTrue(outcome == Outcome.Duplicate, sent.isEmpty, stored.exists(_.status == EventStatus.Resolved))
    },
    // Two acknowledgements in a row cost twice one; the pipe handles a partition's events one at a time
    test("sends the enriched and typed records at once, waiting for one acknowledgement instead of two in a row") {
      ZIO
        .serviceWithZIO[EventHandler](_.handle(Samples.event(eventType = EventType.Sign)))
        .provide(
          TestTelemetry.tracing,
          ZLayer.succeed(kafka),
          InMemoryEventRepo.layer,
          meeting,
          InMemoryIdempotencyCache.layer,
          InMemoryGeoCache.layer,
          InMemoryRegionRepo.layer(Samples.LvivOblast),
          RegionResolver.live,
          DeduplicationService.live,
          EnrichmentService.live,
          routingOnly,
          EventHandler.live,
        )
        .raceFirst(Live.live(ZIO.sleep(5.seconds)) *> ZIO.fail(RuntimeException("the second publish never started")))
        .map(outcome => assertTrue(outcome == Outcome.Processed("signs")))
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
