package com.github.olehbohatyi.mep.gate

import zio.*
import zio.http.*
import zio.json.*

import com.github.olehbohatyi.mep.domain.GeoPoint

object GateRoutes:

  // POST /api/v1/events        — ingest a road event
  // GET  /api/v1/events/nearby — events around lat/lon within radius (km)
  // GET  /api/v1/stats         — aggregated counts, optionally ?region=
  // GET  /api/v1/health        — liveness probe

  private val MaxRadiusKm = 100.0
  private val MaxLimit    = 500

  def apply(ingestion: IngestionService, query: QueryService): Routes[Any, Nothing] =
    Routes(
      Method.GET / "api" / "v1" / "health"            -> handler(Response.text("OK")),
      Method.POST / "api" / "v1" / "events"           -> handler((req: Request) => postEvent(ingestion, req)),
      Method.GET / "api" / "v1" / "events" / "nearby" -> handler((req: Request) => getNearby(query, req)),
      Method.GET / "api" / "v1" / "stats"             -> handler((req: Request) => getStats(query, req)),
    )

  private def json[A: JsonEncoder](status: Status, body: A): Response =
    Response.json(body.toJson).status(status)

  private def badRequest(msg: String): Response = json(Status.BadRequest, ErrorResponse(msg))

  private def internal(op: String)(err: Throwable): UIO[Response] =
    ZIO.logErrorCause(s"$op failed", Cause.fail(err)).as(json(Status.InternalServerError, ErrorResponse("internal")))

  private def postEvent(ingestion: IngestionService, req: Request): UIO[Response] =
    req
      .body
      .asString
      .map(_.fromJson[EventRequest])
      .flatMap {
        case Left(err) => ZIO.succeed(badRequest(s"invalid body: $err"))
        case Right(r)  =>
          ingestion.ingest(r).map {
            case IngestionResult.Accepted(id)  => json(Status.Accepted, IngestResponse(id, "accepted"))
            case IngestionResult.Duplicate(id) => json(Status.Ok, IngestResponse(id, "duplicate"))
          }
      }
      .catchAll(internal("ingest"))

  private def getNearby(query: QueryService, req: Request): UIO[Response] =
    nearbyParams(req) match
      case Left(msg)                      => ZIO.succeed(badRequest(msg))
      case Right((center, radius, limit)) =>
        query.findNearby(center, radius, limit).map(json(Status.Ok, _)).catchAll(internal("nearby query"))

  private def getStats(query: QueryService, req: Request): UIO[Response] =
    val region = req.queryParam("region").filter(_.nonEmpty)
    query.stats(region).map(s => json(Status.Ok, StatsResponse(region, s))).catchAll(internal("stats query"))

  private def nearbyParams(req: Request): Either[String, (GeoPoint, Double, Int)] =
    def double(name: String): Either[String, Option[Double]] =
      req.queryParam(name) match
        case None    => Right(None)
        case Some(v) => v.toDoubleOption.toRight(s"$name must be a number").map(Some(_))
    def int(name: String): Either[String, Option[Int]]       =
      req.queryParam(name) match
        case None    => Right(None)
        case Some(v) => v.toIntOption.toRight(s"$name must be an integer").map(Some(_))
    for
      lat    <- double("lat").flatMap(_.toRight("lat is required"))
      lon    <- double("lon").flatMap(_.toRight("lon is required"))
      radius <- double("radius").map(_.getOrElse(5.0))
      limit  <- int("limit").map(_.getOrElse(50))
      _      <- Either.cond(lat >= -90 && lat <= 90, (), "lat must be within [-90, 90]")
      _      <- Either.cond(lon >= -180 && lon <= 180, (), "lon must be within [-180, 180]")
      _      <- Either.cond(radius > 0 && radius <= MaxRadiusKm, (), s"radius must be within (0, $MaxRadiusKm]")
      _      <- Either.cond(limit > 0 && limit <= MaxLimit, (), s"limit must be within [1, $MaxLimit]")
    yield (GeoPoint(lat, lon), radius, limit)
