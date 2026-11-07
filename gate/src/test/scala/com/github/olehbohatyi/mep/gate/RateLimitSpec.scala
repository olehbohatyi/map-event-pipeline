package com.github.olehbohatyi.mep.gate

import zio.*
import zio.http.*
import zio.metrics.{ Metric, MetricLabel }
import zio.telemetry.opentelemetry.tracing.Tracing
import zio.test.*

import java.net.InetAddress

import com.github.olehbohatyi.mep.domain.GeoPoint
import com.github.olehbohatyi.mep.testkit.{ InMemoryRateLimiter, TestTelemetry }

object RateLimitSpec extends ZIOSpecDefault:

  private val body = """{"eventType":"Hazard","severity":"High","location":{"lat":50.45,"lon":30.52}}"""

  // Counts what reaches the ingestion service, so the tests can see that a limited request publishes nothing
  final private class CountingIngestion(count: Ref[Int]) extends IngestionService:
    def ingest(request: EventRequest, idempotencyKey: Option[String]): Task[IngestionResult] =
      count.update(_ + 1) *> GateRoutesSpec.StubIngestion.ingest(request, idempotencyKey)

  final private case class Gate(routes: Routes[Any, Nothing], ingested: Ref[Int], limiter: InMemoryRateLimiter):
    def send(req: Request, from: String): UIO[Response] =
      ZIO.scoped(routes.runZIO(req.copy(remoteAddress = Some(InetAddress.getByName(from)))))
    def post(from: String): UIO[Response]               =
      send(Request.post(URL(Path.decode("/api/v1/events")), Body.fromString(body)), from)

  private def gate(limit: Int): URIO[Tracing, Gate] =
    for
      tracing  <- ZIO.service[Tracing]
      ingested <- Ref.make(0)
      limiter  <- InMemoryRateLimiter.make(limit)
      calls    <- Ref.make(List.empty[(GeoPoint, Double, Int)])
      routes    = GateRoutes(
                    CountingIngestion(ingested),
                    GateRoutesSpec.StubQuery(calls),
                    GateRoutesSpec.NoFeed,
                    limiter,
                    tracing,
                  )
    yield Gate(routes, ingested, limiter)

  private def decisions(result: String): UIO[Long] =
    Metric.counter("mep_rate_limit_decisions").tagged(MetricLabel("result", result)).value.map(_.count.toLong)

  def spec = suite("Rate limit")(
    test("answers 429 with Retry-After once a client address is over its limit; others are not affected") {
      for
        g        <- gate(limit = 2)
        limited0 <- decisions("limited")
        ok       <- ZIO.foreach(List(1, 2))(_ => g.post("10.0.0.1"))
        over     <- g.post("10.0.0.1")
        overBody <- over.body.asString.orDie
        other    <- g.post("10.0.0.2")
        ingested <- g.ingested.get
        seen     <- g.limiter.seen
        limited1 <- decisions("limited")
      yield assertTrue(
        ok.map(_.status) == List(Status.Accepted, Status.Accepted),
        over.status == Status.TooManyRequests,
        over.rawHeader("Retry-After").contains("42"),
        over.header(Header.ContentType).exists(_.mediaType == MediaType.application.json),
        overBody == """{"error":"rate limit exceeded; retry later"}""",
        other.status == Status.Accepted,
        // The limited request never reached the ingestion service
        ingested == 3,
        seen == Map("10.0.0.1" -> 3, "10.0.0.2" -> 1),
        limited1 - limited0 >= 1L,
      )
    },
    test("only event submissions are limited") {
      for
        g      <- gate(limit = 0)
        post   <- g.post("10.0.0.1")
        health <- g.send(Request.get(URL(Path.decode("/api/v1/health"))), "10.0.0.1")
        recent <- g.send(Request.get(URL(Path.decode("/api/v1/events/recent"))), "10.0.0.1")
        seen   <- g.limiter.seen
      yield assertTrue(
        post.status == Status.TooManyRequests,
        health.status == Status.Ok,
        recent.status == Status.Ok,
        seen == Map("10.0.0.1" -> 1),
      )
    },
    test("lets requests through when the limiter fails, rather than refusing every event") {
      for
        g        <- gate(limit = 0)
        _        <- g.limiter.fail
        res      <- g.post("10.0.0.1")
        ingested <- g.ingested.get
      yield assertTrue(res.status == Status.Accepted, ingested == 1)
    },
    // Regression: only a failed call was let through; a Redis that never answered held every submission
    test("lets requests through when the limiter does not answer in time") {
      for
        g        <- gate(limit = 0)
        _        <- g.limiter.hang
        fiber    <- g.post("10.0.0.1").fork
        _        <- TestClock.adjust(GateRoutes.LimiterTimeout)
        res      <- fiber.join
        ingested <- g.ingested.get
      yield assertTrue(res.status == Status.Accepted, ingested == 1)
    },
  ).provide(TestTelemetry.tracing)
