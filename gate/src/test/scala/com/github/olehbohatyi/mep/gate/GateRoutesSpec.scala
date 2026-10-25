package com.github.olehbohatyi.mep.gate

import zio.*
import zio.http.*
import zio.test.*

import java.time.Instant
import java.util.UUID

import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.stores.MapEventStat
import com.github.olehbohatyi.mep.testkit.Samples

object GateRoutesSpec extends ZIOSpecDefault:

  private val fixedId = UUID.fromString("00000000-0000-0000-0000-000000000001")

  private object StubIngestion extends IngestionService:
    def ingest(request: EventRequest, idempotencyKey: Option[String]): Task[IngestionResult] =
      idempotencyKey match
        case Some("dup")  => ZIO.succeed(IngestionResult.Duplicate(fixedId))
        case Some("boom") => ZIO.fail(RuntimeException("kafka down"))
        case _            => ZIO.succeed(IngestionResult.Accepted(fixedId))

  final private class StubQuery(calls: Ref[List[(GeoPoint, Double, Int)]]) extends QueryService:
    def findNearby(center: GeoPoint, radiusKm: Double, limit: Int): Task[NearbyResponse] =
      calls.update(_ :+ (center, radiusKm, limit)).as(NearbyResponse("zone_50_30", 1, List(Samples.event())))
    def stats(region: Option[String]): Task[List[MapEventStat]]                          =
      ZIO.succeed(List(MapEventStat(region.getOrElse("all"), EventType.Hazard, 3, Instant.EPOCH)))

  private val validBody =
    """{"eventType":"Hazard","severity":"High","location":{"lat":50.45,"lon":30.52},"description":"ice"}"""

  private def call(req: Request): ZIO[Ref[List[(GeoPoint, Double, Int)]], Nothing, (Status, String)] =
    for
      calls <- ZIO.service[Ref[List[(GeoPoint, Double, Int)]]]
      res   <-
        ZIO.scoped {
          GateRoutes(StubIngestion, StubQuery(calls)).runZIO(req).flatMap(r => r.body.asString.orDie.map(r.status -> _))
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
        missing._2.contains("lat is required"),
      )
    },
    test("stats by region") {
      call(get("/api/v1/stats?region=zone_1_2"))
        .map((s, b) => assertTrue(s == Status.Ok, b.contains("\"region\":\"zone_1_2\""), b.contains("\"count\":3")))
    },
  ).provide(ZLayer(Ref.make(List.empty[(GeoPoint, Double, Int)])))
