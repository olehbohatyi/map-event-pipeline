package com.github.olehbohatyi.mep.gate

import zio.*
import zio.http.*
import zio.json.*
import zio.stream.{ ZPipeline, ZStream }
import zio.telemetry.opentelemetry.tracing.Tracing
import zio.test.*

import java.time.Instant
import java.util.UUID

import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.stores.{ MapEventStat, MapEventTrend }
import com.github.olehbohatyi.mep.testkit.{ Samples, TestTelemetry }

object GateRoutesSpec extends ZIOSpecDefault:

  private val fixedId = UUID.fromString("00000000-0000-0000-0000-000000000001")

  object StubIngestion extends IngestionService:
    def ingest(request: EventRequest, idempotencyKey: Option[String]): Task[IngestionResult] =
      idempotencyKey match
        case Some("dup")  => ZIO.succeed(IngestionResult.Duplicate(fixedId))
        case Some("boom") => ZIO.fail(RuntimeException("kafka down"))
        case _            => ZIO.succeed(IngestionResult.Accepted(fixedId))

  final class StubQuery(calls: Ref[List[(GeoPoint, Double, Int)]]) extends QueryService:
    def findNearby(center: GeoPoint, radiusKm: Double, limit: Int): Task[NearbyResponse] =
      calls.update(_ :+ (center, radiusKm, limit)).as(NearbyResponse("zone_50_30", 1, List(Samples.event())))
    // Only the fixed id is stored
    def find(id: UUID): Task[Option[MapEvent]]                                           =
      ZIO.succeed(Option.when(id == fixedId)(Samples.event().copy(id = fixedId)))
    // Echoes the requested limit as the count, so tests can see what reached the service
    def recent(limit: Int): Task[RecentResponse]                                         =
      ZIO.succeed(RecentResponse(limit, Nil))
    def stats(region: Option[String]): Task[List[MapEventStat]]                          =
      ZIO.succeed(List(MapEventStat(region.getOrElse("all"), EventType.Hazard, 3, Instant.EPOCH)))
    // One row per requested hour, so tests can see what reached the service
    def hourly(region: Option[String], hours: Int): Task[List[MapEventTrend]]            =
      ZIO.succeed(
        List.fill(hours)(MapEventTrend(Instant.EPOCH, region.getOrElse("all"), EventType.Sign, 1, Instant.EPOCH))
      )

  // For the tests that do not stream: no events, no subscribers
  object NoFeed extends LiveFeed:
    def subscribe(area: Option[BoundingBox]): ZStream[Any, Nothing, MapEvent] = ZStream.never
    def subscribers: UIO[Int]                                                 = ZIO.succeed(0)
    def close: UIO[Unit]                                                      = ZIO.unit

  private val validBody =
    """{"eventType":"Hazard","severity":"High","location":{"lat":50.45,"lon":30.52},"description":"ice"}"""

  private def call(req: Request): ZIO[Ref[List[(GeoPoint, Double, Int)]] & Tracing, Nothing, (Status, String)] =
    for
      calls   <- ZIO.service[Ref[List[(GeoPoint, Double, Int)]]]
      tracing <- ZIO.service[Tracing]
      res     <-
        ZIO.scoped {
          GateRoutes(StubIngestion, StubQuery(calls), NoFeed, tracing)
            .runZIO(req)
            .flatMap(r => r.body.asString.orDie.map(r.status -> _))
        }
    yield res

  private def get(path: String) = Request.get(URL.decode(path).toOption.get)

  def spec = suite("GateRoutes")(
    test("health") {
      call(get("/api/v1/health")).map((s, b) => assertTrue(s == Status.Ok, b == "OK"))
    },
    test("accepts a valid event with 202") {
      call(Request.post(URL(Path.decode("/api/v1/events")), Body.fromString(validBody)))
        .map((s, b) => assertTrue(s == Status.Accepted, b.contains("\"status\":\"accepted\"")))
    },
    test("reports a duplicate Idempotency-Key with 200") {
      val req =
        Request.post(URL(Path.decode("/api/v1/events")), Body.fromString(validBody)).addHeader("Idempotency-Key", "dup")
      call(req).map((s, b) => assertTrue(s == Status.Ok, b.contains("\"status\":\"duplicate\"")))
    },
    test("rejects an invalid body with 400 and well-formed JSON") {
      call(Request.post(URL(Path.decode("/api/v1/events")), Body.fromString("""{"eventType":"Meteor"}""")))
        .map((s, b) =>
          assertTrue(
            s == Status.BadRequest,
            b.startsWith("{\"error\":"),
            zio.json.ast.Json.decoder.decodeJson(b).isRight,
          )
        )
    },
    // Regression: occurredAt is client input; a far-future one stayed on top of /recent and on the map for good
    test("rejects an occurredAt more than five minutes in the future, accepts one within the skew") {
      val now                    = java.time.Instant.parse("2026-10-27T12:00:00Z")
      def at(occurredAt: String) =
        call(
          Request.post(
            URL(Path.decode("/api/v1/events")),
            Body.fromString(validBody.dropRight(1) + s""","occurredAt":"$occurredAt"}"""),
          )
        )
      for
        _      <- TestClock.setTime(now)
        future <- at("2026-10-27T12:05:01Z")
        skewed <- at("2026-10-27T12:04:59Z")
        past   <- at("2026-10-20T08:00:00Z")
      yield assertTrue(
        future == (Status.BadRequest, """{"error":"occurredAt must not be more than 5 minutes in the future"}"""),
        skewed._1 == Status.Accepted,
        past._1 == Status.Accepted,
      )
    },
    test("maps ingestion failures to 500") {
      val req = Request
        .post(URL(Path.decode("/api/v1/events")), Body.fromString(validBody))
        .addHeader("Idempotency-Key", "boom")
      call(req).map((s, b) => assertTrue(s == Status.InternalServerError, b == """{"error":"internal"}"""))
    },
    test("nearby passes parsed params with defaults") {
      for
        res   <- call(get("/api/v1/events/nearby?lat=50.45&lon=30.52"))
        calls <- ZIO.serviceWithZIO[Ref[List[(GeoPoint, Double, Int)]]](_.get)
      yield assertTrue(
        res._1 == Status.Ok,
        res._2.contains("\"region\":\"zone_50_30\""),
        calls == List((GeoPoint(50.45, 30.52), 5.0, 50)),
      )
    },
    test("nearby validates params") {
      for
        missing <- call(get("/api/v1/events/nearby?lon=30"))
        badLat  <- call(get("/api/v1/events/nearby?lat=91&lon=30"))
        badNum  <- call(get("/api/v1/events/nearby?lat=abc&lon=30"))
        radius  <- call(get("/api/v1/events/nearby?lat=1&lon=1&radius=1000"))
        limit   <- call(get("/api/v1/events/nearby?lat=1&lon=1&limit=0"))
      yield assertTrue(
        List(missing, badLat, badNum, radius, limit).forall(_._1 == Status.BadRequest),
        missing._2.contains("Missing query parameters lat"),
        badNum._2.startsWith("{\"error\":"),
        badLat._2 == """{"error":"lat must be within [-90, 90]"}""",
      )
    },
    test("keeps the established JSON shape: absent optionals (here the region) are omitted, enums are plain strings") {
      call(get("/api/v1/events/nearby?lat=50.45&lon=30.52")).map { (_, b) =>
        val event = zio.json.ast.Json.decoder.decodeJson(b).toOption.get
        val first = event.asObject.flatMap(_.get("events")).flatMap(_.asArray).flatMap(_.headOption).flatMap(_.asObject)
        assertTrue(
          first.exists(!_.contains("region")),
          first.flatMap(_.get("eventType")).contains(zio.json.ast.Json.Str("Hazard")),
        )
      }
    },
    test("rewrites unreadable requests to the {\"error\": ...} shape") {
      for
        badEnum <- call(Request.post(URL(Path.decode("/api/v1/events")), Body.fromString("""{"eventType":"Meteor"}""")))
        badJson <- call(Request.post(URL(Path.decode("/api/v1/events")), Body.fromString("{oops")))
      yield assertTrue(
        badEnum._1 == Status.BadRequest,
        badJson._1 == Status.BadRequest,
        badEnum._2.startsWith("{\"error\":\"invalid request:"),
        badJson._2.startsWith("{\"error\":\"invalid request:"),
      )
    },
    // Regression: before the Endpoint API the gate read every body as JSON; `curl -d` sends form-urlencoded
    test("reads the body as JSON whatever Content-Type the client sends") {
      // The server attaches the header's media type to the body, and that is what decoding looks at
      def post(contentType: MediaType) =
        call(
          Request
            .post(URL(Path.decode("/api/v1/events")), Body.fromString(validBody).contentType(contentType))
            .addHeader(Header.ContentType(contentType))
        )
      for
        form  <- post(MediaType.application.`x-www-form-urlencoded`)
        plain <- post(MediaType.text.plain)
        json  <- post(MediaType.application.json)
      yield assertTrue(List(form, plain, json).forall(_._1 == Status.Accepted))
    },
    // Regression: zio-http rendered its decode errors as an HTML page when the client accepted HTML
    test("reports unreadable requests as JSON even to clients that accept HTML") {
      val html = Header.Accept(MediaType.text.html)
      for
        badJson <- call(Request.post(URL(Path.decode("/api/v1/events")), Body.fromString("{oops")).addHeader(html))
        badNum  <- call(get("/api/v1/events/nearby?lat=abc&lon=30").addHeader(html))
        missing <- call(get("/api/v1/events/nearby?lon=30").addHeader(html))
      yield assertTrue(
        List(badJson, badNum, missing).forall(_._1 == Status.BadRequest),
        List(badJson, badNum, missing).forall(_._2.startsWith("{\"error\":\"invalid request:")),
      )
    },
    // Guards the wiring above for endpoints added later (`stats` has no input that can fail to decode)
    test("every endpoint answers decode errors with the JSON codec") {
      assertTrue(GateApi.endpoints.forall(_.codecError eq GateApi.invalidRequest))
    },
    test("serves the Swagger UI over the OpenAPI document") {
      call(get("/docs/openapi")).map((s, b) => assertTrue(s == Status.Ok, b.contains("SwaggerUI")))
    },
    test("recent uses a default limit and validates it") {
      for
        default <- call(get("/api/v1/events/recent"))
        seven   <- call(get("/api/v1/events/recent?limit=7"))
        zero    <- call(get("/api/v1/events/recent?limit=0"))
        tooMany <- call(get("/api/v1/events/recent?limit=501"))
      yield assertTrue(
        default == (Status.Ok, """{"count":200,"events":[]}"""),
        seven == (Status.Ok, """{"count":7,"events":[]}"""),
        zero == (Status.BadRequest, """{"error":"limit must be within [1, 500]"}"""),
        tooMany._1 == Status.BadRequest,
      )
    },
    test("live rejects a malformed bbox before streaming") {
      for
        short   <- call(get("/api/v1/events/live?bbox=30,50,31"))
        swapped <- call(get("/api/v1/events/live?bbox=31,50,30,51"))
      yield assertTrue(
        short == (Status.BadRequest, """{"error":"bbox must be four numbers: minLon,minLat,maxLon,maxLat"}"""),
        swapped == (Status.BadRequest, """{"error":"bbox minimum must not exceed its maximum"}"""),
      )
    },
    test("live streams events inside the bbox as server-sent events, and ends when the feed closes") {
      val kyiv = Samples.event()
      val lviv = Samples.event(location = GeoPoint(49.84, 24.03))
      ZIO.scoped {
        for
          source <- Queue.unbounded[MapEvent]
          feed   <- LiveFeed.fromSource(ZStream.fromQueue(source))
          calls  <- Ref.make(List.empty[(GeoPoint, Double, Int)])
          res    <- GateRoutes
                      .api(StubIngestion, StubQuery(calls), feed)
                      .runZIO(get("/api/v1/events/live?bbox=30,50,31,51"))
          seen   <- Ref.make("")
          reader <- res.body.asStream.via(ZPipeline.utf8Decode).runForeach(s => seen.update(_ + s)).fork
          _      <- feed.subscribers.repeatUntil(_ == 1)
          _      <- source.offerAll(List(lviv, kyiv))
          // Close only once the client has the event; then the stream must end on its own
          _      <- seen.get.repeatUntil(_.contains(kyiv.id.toString))
          _      <- feed.close
          ended  <- reader.join.timeout(5.seconds).withClock(Clock.ClockLive)
          body   <- seen.get
          after  <- feed.subscribers
        yield assertTrue(
          res.status == Status.Ok,
          res.header(Header.ContentType).exists(_.mediaType == MediaType.text.`event-stream`),
          body.contains(s"event: event\ndata: ${kyiv.toJson}\nid: ${kyiv.id}\n"),
          !body.contains(lviv.id.toString),
          ended.isDefined,
          after == 0,
        )
      }
    },
    test("serves the live map page as HTML") {
      ZIO.scoped {
        for
          calls   <- ZIO.service[Ref[List[(GeoPoint, Double, Int)]]]
          tracing <- ZIO.service[Tracing]
          res     <- GateRoutes(StubIngestion, StubQuery(calls), NoFeed, tracing).runZIO(get("/map"))
          body    <- res.body.asString.orDie
        yield assertTrue(
          res.status == Status.Ok,
          res.header(Header.ContentType).exists(_.mediaType == MediaType.text.html),
          // Regression: jar entries carry a fixed 2010 timestamp, so without this browsers kept the page from an
          // older build after a deploy (heuristic freshness from Last-Modified)
          res.header(Header.CacheControl).contains(Header.CacheControl.NoStore),
          body.contains("new EventSource(\"/api/v1/events/live\")"),
        )
      }
    },
    test("an event is found by id once stored, 404 otherwise, 400 for a malformed id") {
      for
        found   <- call(get(s"/api/v1/events/$fixedId"))
        missing <- call(get("/api/v1/events/00000000-0000-0000-0000-000000000002"))
        bad     <- call(get("/api/v1/events/not-a-uuid"))
      yield assertTrue(
        found._1 == Status.Ok,
        found._2.contains(s"\"id\":\"$fixedId\""),
        missing == (Status.NotFound, """{"error":"no stored event 00000000-0000-0000-0000-000000000002"}"""),
        bad._1 == Status.NotFound || bad._1 == Status.BadRequest,
      )
    },
    test("hourly uses a default of 24 hours and validates the range") {
      for
        default <- call(get("/api/v1/stats/hourly?region=Kyiv%20City"))
        two     <- call(get("/api/v1/stats/hourly?hours=2"))
        zero    <- call(get("/api/v1/stats/hourly?hours=0"))
        tooMany <- call(get("/api/v1/stats/hourly?hours=169"))
      yield assertTrue(
        default._1 == Status.Ok,
        default._2.startsWith("""{"region":"Kyiv City","hours":24,"counts":[{"windowStart":"1970-01-01T00:00:00Z""""),
        default._2.split("windowStart").length == 25,
        two._2.startsWith("""{"hours":2,"counts":["""),
        zero == (Status.BadRequest, """{"error":"hours must be within [1, 168]"}"""),
        tooMany._1 == Status.BadRequest,
      )
    },
    test("stats by region") {
      call(get("/api/v1/stats?region=zone_1_2"))
        .map((s, b) => assertTrue(s == Status.Ok, b.contains("\"region\":\"zone_1_2\""), b.contains("\"count\":3")))
    },
  ).provide(ZLayer(Ref.make(List.empty[(GeoPoint, Double, Int)])), TestTelemetry.tracing)
