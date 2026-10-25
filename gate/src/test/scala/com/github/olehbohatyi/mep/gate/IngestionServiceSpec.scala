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
    test("publishes a raw event to the raw topic") {
      for
        svc      <- ZIO.service[IngestionService]
        producer <- ZIO.service[RecordingProducer]
        result   <- svc.ingest(request)
        sent     <- producer.all
      yield assertTrue(
        sent.map(_._1) == Chunk("raw"),
        result == IngestionResult.Accepted(sent.head._2.id),
        sent.head._2.status == EventStatus.Raw,
      )
    },
    test("every request is a new event") {
      for
        svc      <- ZIO.service[IngestionService]
        producer <- ZIO.service[RecordingProducer]
        _        <- svc.ingest(request).repeatN(2)
        sent     <- producer.all
      yield assertTrue(sent.size == 3, sent.map(_._2.id).distinct.size == 3)
    },
    test("keeps the client's occurredAt") {
      val at = java.time.Instant.parse("2025-05-05T05:05:05Z")
      for
        svc      <- ZIO.service[IngestionService]
        producer <- ZIO.service[RecordingProducer]
        _        <- svc.ingest(request.copy(occurredAt = Some(at)))
        sent     <- producer.all
      yield assertTrue(sent.head._2.occurredAt == at)
    },
  ).provide(RecordingProducer.layer, InMemoryIdempotencyCache.layer, ZLayer.succeed(kafka), IngestionService.live)
