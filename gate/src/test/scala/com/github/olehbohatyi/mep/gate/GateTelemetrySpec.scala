package com.github.olehbohatyi.mep.gate

import zio.*
import zio.http.*
import zio.metrics.{ Metric, MetricLabel }
import zio.telemetry.opentelemetry.tracing.Tracing
import zio.test.*

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.{ SpanKind, StatusCode }
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.data.SpanData
import scala.jdk.CollectionConverters.*
import com.github.olehbohatyi.mep.domain.GeoPoint
import com.github.olehbohatyi.mep.testkit.TestTelemetry

object GateTelemetrySpec extends ZIOSpecDefault:

  private val traceId = "4bf92f3577b34da6a3ce929d0e0e4736"
  private val caller  = "00f067aa0ba902b7"

  private val body =
    """{"eventType":"Hazard","severity":"High","location":{"lat":50.45,"lon":30.52}}"""

  // Merged into an empty app first, as zio-http's server does (NettyDriver.addApp: `oldApp ++ newApp`, and `++`
  // keeps the left side's not-found handler)
  private def run(req: Request): ZIO[Tracing, Nothing, Response] =
    ZIO.serviceWithZIO[Tracing] { tracing =>
      Ref
        .make(List.empty[(GeoPoint, Double, Int)])
        .flatMap { calls =>
          val routes =
            GateRoutes(GateRoutesSpec.StubIngestion, GateRoutesSpec.StubQuery(calls), GateRoutesSpec.NoFeed, tracing)
          ZIO.scoped((Routes.empty ++ routes).runZIO(req))
        }
    }

  private def post(idempotencyKey: String) =
    Request
      .post(URL(Path.decode("/api/v1/events")), Body.fromString(body))
      .addHeader("traceparent", s"00-$traceId-$caller-01")
      .addHeader("Idempotency-Key", idempotencyKey)

  private def serverSpans: URIO[InMemorySpanExporter, List[SpanData]] =
    ZIO.serviceWith[InMemorySpanExporter](_.getFinishedSpanItems.asScala.toList.filter(_.getKind == SpanKind.SERVER))

  private def requests(route: String, status: Int): UIO[Long] =
    Metric
      .counter("mep_http_server_requests")
      .tagged(Set(MetricLabel("method", "GET"), MetricLabel("route", route), MetricLabel("status", status.toString)))
      .value
      .map(_.count.toLong)

  def spec = suite("Gate telemetry")(
    test("a request continues the caller's trace in a server span that records the status") {
      for
        _     <- ZIO.serviceWith[InMemorySpanExporter](_.reset())
        res   <- run(post("new"))
        spans <- serverSpans
      yield assertTrue(
        res.status == Status.Accepted,
        spans.map(_.getName) == List("POST /api/v1/events"),
        spans.forall(_.getTraceId == traceId),
        spans.forall(_.getParentSpanId == caller),
        spans.forall(_.getAttributes.get(AttributeKey.longKey("http.response.status_code")) == 202L),
        spans.forall(_.getStatus.getStatusCode == StatusCode.UNSET),
      )
    },
    test("a server error marks the span as failed") {
      for
        _     <- ZIO.serviceWith[InMemorySpanExporter](_.reset())
        res   <- run(post("boom"))
        spans <- serverSpans
      yield assertTrue(
        res.status == Status.InternalServerError,
        spans.map(_.getStatus.getStatusCode) == List(StatusCode.ERROR),
      )
    },
    // Regression: on the server, unknown paths were neither traced nor counted (the wrapped not-found handler was lost)
    test("requests are counted by route and status; unknown paths share one label") {
      for
        health0 <- requests("/api/v1/health", 200)
        other0  <- requests("unmatched", 404)
        _       <- run(Request.get(URL(Path.decode("/api/v1/health"))))
        _       <- run(Request.get(URL(Path.decode("/wp-login.php"))))
        _       <- run(Request.get(URL(Path.decode("/.env"))))
        health1 <- requests("/api/v1/health", 200)
        other1  <- requests("unmatched", 404)
      yield assertTrue(health1 - health0 == 1L, other1 - other0 == 2L)
    },
  ).provide(TestTelemetry.recording) @@ TestAspect.sequential
