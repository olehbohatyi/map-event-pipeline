package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.test.*

import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import scala.jdk.CollectionConverters.*
import com.github.olehbohatyi.mep.broker.EventCodec
import com.github.olehbohatyi.mep.geo.RegionResolver
import com.github.olehbohatyi.mep.testkit.*

object PipeTelemetrySpec extends ZIOSpecDefault:

  private val traceId = "4bf92f3577b34da6a3ce929d0e0e4736"
  private val gate    = "00f067aa0ba902b7"

  private def record(headers: Map[String, String]) =
    val event = Samples.event()
    RawRecord("raw", 0, 7L, Some(event.id.toString), EventCodec.encode(event), headers)

  def spec = suite("Pipe telemetry")(
    test("processing continues the trace the record's headers carry, with a span per step") {
      for
        spans   <- ZIO.service[InMemorySpanExporter]
        _       <- ZIO.succeed(spans.reset())
        handler <- ZIO.service[RecordHandler]
        _       <- handler.handle(record(Map("traceparent" -> s"00-$traceId-$gate-01")))
        all      = spans.getFinishedSpanItems.asScala.toList
        process  = all.filter(_.getName == "raw process")
        steps    = all.filter(s => process.exists(_.getSpanId == s.getParentSpanId)).map(_.getName).toSet
      yield assertTrue(
        process.size == 1,
        process.forall(_.getKind == SpanKind.CONSUMER),
        process.forall(_.getParentSpanId == gate),
        all.forall(_.getTraceId == traceId),
        steps == Set("dedup check", "enrich", "escalate", "store", "dedup mark"),
      )
    },
    test("a record without trace headers starts its own trace") {
      for
        spans   <- ZIO.service[InMemorySpanExporter]
        _       <- ZIO.succeed(spans.reset())
        handler <- ZIO.service[RecordHandler]
        _       <- handler.handle(record(Map.empty))
        process  = spans.getFinishedSpanItems.asScala.toList.filter(_.getName == "raw process")
      yield assertTrue(process.size == 1, process.forall(p => !p.getParentSpanContext.isValid))
    },
  ).provide(
    TestTelemetry.recording,
    ZLayer.succeed(EventHandlerSpec.kafka),
    InMemoryEventRepo.layer,
    RecordingProducer.layer,
    InMemoryIdempotencyCache.layer,
    InMemoryGeoCache.layer,
    InMemoryRegionRepo.layer(Samples.LvivOblast),
    RegionResolver.live,
    DeduplicationService.live,
    EnrichmentService.live,
    EventHandlerSpec.routingOnly,
    EventHandler.live,
    RecordHandler.live,
  ) @@ TestAspect.sequential
