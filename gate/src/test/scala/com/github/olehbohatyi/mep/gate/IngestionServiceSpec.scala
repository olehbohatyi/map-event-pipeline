package com.github.olehbohatyi.mep.gate

import zio.*
import zio.test.*

import com.github.olehbohatyi.mep.config.{ KafkaConfig, TopicsConfig }
import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.testkit.*

object IngestionServiceSpec extends ZIOSpecDefault:

  val kafka   = KafkaConfig("unused:9092", "g", TopicsConfig("raw", "enriched", "hazards", "signs", "traffic"))
  val request = EventRequest(EventType.Sign, Severity.Medium, Samples.Kyiv, Some("stop sign down"), None)

  def spec = suite("IngestionService")(
    test("same Idempotency-Key yields the same id and publishes once") {
      for
        svc      <- ZIO.service[IngestionService]
        producer <- ZIO.service[RecordingProducer]
        first    <- svc.ingest(request, Some("req-1"))
        second   <- svc.ingest(request, Some("req-1"))
        sent     <- producer.all
      yield assertTrue(
        first == IngestionResult.Accepted(IngestionService.eventId("req-1")),
        second == IngestionResult.Duplicate(IngestionService.eventId("req-1")),
        sent.map(_._1) == Chunk("raw"),
        sent.head._2.id == IngestionService.eventId("req-1"),
        sent.head._2.status == EventStatus.Raw,
      )
    },
    test("without a key every request is a new event") {
      for
        svc      <- ZIO.service[IngestionService]
        producer <- ZIO.service[RecordingProducer]
        _        <- svc.ingest(request, None).repeatN(2)
        sent     <- producer.all
      yield assertTrue(sent.size == 3, sent.map(_._2.id).distinct.size == 3)
    },
    test("a failed publish releases the key so the client can retry") {
      for
        svc      <- ZIO.service[IngestionService]
        producer <- ZIO.service[RecordingProducer]
        _        <- producer.failNext(1)
        failed   <- svc.ingest(request, Some("req-2")).exit
        retried  <- svc.ingest(request, Some("req-2"))
      yield assertTrue(failed.isFailure, retried == IngestionResult.Accepted(IngestionService.eventId("req-2")))
    },
    test("keeps the client's occurredAt") {
      val at = java.time.Instant.parse("2025-05-05T05:05:05Z")
      for
        svc      <- ZIO.service[IngestionService]
        producer <- ZIO.service[RecordingProducer]
        _        <- svc.ingest(request.copy(occurredAt = Some(at)), None)
        sent     <- producer.all
      yield assertTrue(sent.head._2.occurredAt == at)
    },
  ).provide(RecordingProducer.layer, InMemoryIdempotencyCache.layer, ZLayer.succeed(kafka), IngestionService.live)
