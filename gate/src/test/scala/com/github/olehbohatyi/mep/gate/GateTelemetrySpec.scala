package com.github.olehbohatyi.mep.gate

import zio.*
import zio.http.*
import zio.metrics.{ Metric, MetricLabel }
import zio.telemetry.opentelemetry.tracing.Tracing
import zio.test.*

import java.util.UUID

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

  private def requests(method: Method, route: String, status: Int): UIO[Long] =
    Metric
      .counter("mep_http_server_requests")
      .tagged(
        Set(MetricLabel("method", method.toString), MetricLabel("route", route), MetricLabel("status", status.toString))
      )
      .value
      .map(_.count.toLong)

  // What the request metric has seen so far in this JVM, from any spec
  private def labels(name: String): UIO[Set[String]] =
    ZIO
      .metrics
      .map(_.metrics.filter(_.metricKey.name == "mep_http_server_requests").flatMap(_.metricKey.tags))
      .map(_.filter(_.key == name).map(_.value))

  private def get(path: String): ZIO[Tracing, Nothing, Response] =
    run(Request.get(URL.decode(path).toOption.get))

  private val eventRoute = "/api/v1/events/{id}"

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
    // Metrics are global and other specs run in parallel: DELETE is used by no other spec, so its count is exact, and
    // GateRoutesSpec also calls health
    test("requests are counted by route and status; unknown paths share one label") {
      for
        health0 <- requests(Method.GET, "/api/v1/health", 200)
        other0  <- requests(Method.DELETE, "unmatched", 404)
        _       <- run(Request.get(URL(Path.decode("/api/v1/health"))))
        _       <- run(Request.delete(URL(Path.decode("/wp-login.php"))))
        _       <- run(Request.delete(URL(Path.decode("/.env"))))
        health1 <- requests(Method.GET, "/api/v1/health", 200)
        other1  <- requests(Method.DELETE, "unmatched", 404)
      yield assertTrue(health1 - health0 >= 1L, other1 - other0 == 2L)
    },
    // Regression: a stored event was labelled by its own path (a series per id), a missing one as an unmatched path.
    // GateRoutesSpec calls this route too, so these counts are lower bounds; the next test rules out concrete paths
    test("a route is labelled by its template, whatever the id and the status") {
      for
        stored0  <- requests(Method.GET, eventRoute, 200)
        missing0 <- requests(Method.GET, eventRoute, 404)
        stored   <- get(s"/api/v1/events/${GateRoutesSpec.fixedId}")
        missing  <- get(s"/api/v1/events/${UUID.randomUUID()}")
        stored1  <- requests(Method.GET, eventRoute, 200)
        missing1 <- requests(Method.GET, eventRoute, 404)
      yield assertTrue(
        stored.status == Status.Ok,
        missing.status == Status.NotFound,
        stored1 - stored0 >= 1L,
        missing1 - missing0 >= 1L,
      )
    },
    // Every spec's requests count here, so this holds exactly. Non-standard methods match no route (Method.ANY covers the
    // standard ones), so they never reach the metric and cannot grow the method label.
    test("the route and method labels are a fixed set, whatever clients send") {
      val routes = Set(
        "/api/v1/health",
        "/api/v1/events",
        "/api/v1/events/nearby",
        eventRoute,
        "/api/v1/events/recent",
        "/api/v1/events/live",
        "/api/v1/stats",
        "/api/v1/stats/hourly",
        "/docs/openapi",
        "/docs/openapi/Map+Event+Pipeline+API.json",
        "/map",
        "unmatched",
      )
      for
        _       <-
          ZIO.foreachDiscard(routes.filterNot(_ == "unmatched"))(p => get(p.replace("{id}", UUID.randomUUID.toString)))
        _       <- run(post("labels"))
        _       <- get("/api/v1/events/not-a-uuid")
        _       <- run(Request(method = Method.CUSTOM("BREW"), url = URL(Path.decode("/api/v1/health"))))
        _       <- run(Request(method = Method.CUSTOM("BREW"), url = URL(Path.decode("/coffee"))))
        seen    <- labels("route")
        methods <- labels("method")
      yield assertTrue(seen == routes, !methods("BREW"))
    },
  ).provide(TestTelemetry.recording) @@ TestAspect.sequential
