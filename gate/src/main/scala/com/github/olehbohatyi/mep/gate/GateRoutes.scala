package com.github.olehbohatyi.mep.gate

import zio.*
import zio.http.*
import zio.http.codec.PathCodec
import zio.http.endpoint.openapi.SwaggerUI
import zio.json.*

import com.github.olehbohatyi.mep.domain.GeoPoint

object GateRoutes:

  // POST /api/v1/events        — ingest a road event (optional Idempotency-Key header)
  // GET  /api/v1/events/nearby — events around lat/lon within radius (km)
  // GET  /api/v1/stats         — aggregated counts, optionally ?region=
  // GET  /api/v1/health        — liveness probe
  // GET  /docs/openapi         — Swagger UI over the generated OpenAPI document (assets come from unpkg.com)

  private val MaxRadiusKm = 100.0
  private val MaxLimit    = 500

  def apply(ingestion: IngestionService, query: QueryService): Routes[Any, Nothing] =
    api(ingestion, query) ++ SwaggerUI.routes(PathCodec.path("docs") / "openapi", GateApi.openApi).handleError(identity)

  // The API routes alone, without the docs; request-decoding failures are reported as `{"error": ...}` like all others
  def api(ingestion: IngestionService, query: QueryService): Routes[Any, Nothing] =
    Routes(
      GateApi.health.implement(_ => ZIO.succeed("OK")),
      GateApi.ingest.implement((key, req) => postEvent(ingestion, req, key)),
      GateApi.nearby.implement((lat, lon, radius, limit) => getNearby(query, lat, lon, radius, limit)),
      GateApi.stats.implement(region => getStats(query, region)),
    ).transform(_.mapZIO(normalizeDecodeError))

  private def internal(op: String)(err: Throwable): UIO[ApiError.Internal] =
    ZIO.logErrorCause(s"$op failed", Cause.fail(err)).as(ApiError.Internal("internal"))

  private def postEvent(
      ingestion: IngestionService,
      req: EventRequest,
      idempotencyKey: Option[String],
    ): IO[ApiError, Either[IngestDuplicate, IngestAccepted]] =
    ingestion
      .ingest(req, idempotencyKey.map(_.trim).filter(_.nonEmpty))
      .mapBoth(
        err => err,
        {
          case IngestionResult.Accepted(id)  => Right(IngestAccepted(id, "accepted"))
          case IngestionResult.Duplicate(id) => Left(IngestDuplicate(id, "duplicate"))
        },
      )
      .tapError(err => internal("ingest")(err).unit)
      .mapError(_ => ApiError.Internal("internal"))

  private def getNearby(
      query: QueryService,
      lat: Double,
      lon: Double,
      radius: Option[Double],
      limit: Option[Int],
    ): IO[ApiError, NearbyResponse] =
    val r       = radius.getOrElse(5.0)
    val l       = limit.getOrElse(50)
    val invalid =
      if lat < -90 || lat > 90 then Some("lat must be within [-90, 90]")
      else if lon < -180 || lon > 180 then Some("lon must be within [-180, 180]")
      else if !(r > 0 && r <= MaxRadiusKm) then Some(s"radius must be within (0, $MaxRadiusKm]")
      else if l <= 0 || l > MaxLimit then Some(s"limit must be within [1, $MaxLimit]")
      else None
    invalid match
      case Some(msg) => ZIO.fail(ApiError.BadRequest(msg))
      case None      => query.findNearby(GeoPoint(lat, lon), r, l).catchAll(internalFail("nearby query"))

  private def getStats(query: QueryService, region: Option[String]): IO[ApiError.Internal, StatsResponse] =
    val filter = region.filter(_.nonEmpty)
    query.stats(filter).map(StatsResponse(filter, _)).catchAll(internalFail("stats query"))

  private def internalFail(op: String)(err: Throwable): IO[ApiError.Internal, Nothing] =
    internal(op)(err).flatMap(ZIO.fail(_))

  // zio-http reports undecodable input (bad JSON, unknown enum value, missing/malformed query parameter) as
  // `{"name": ..., "message": ...}`; clients only ever see the `{"error": ...}` shape
  private def normalizeDecodeError(res: Response): UIO[Response] =
    if res.status != Status.BadRequest then ZIO.succeed(res)
    else
      ZIO.scoped(res.body.asString.orElseSucceed("")).map { body =>
        body.fromJson[DecodeFailure] match
          case Right(failure) => json(Status.BadRequest, ApiError.BadRequest(s"invalid request: ${failure.message}"))
          case Left(_)        => res.copy(body = Body.fromString(body))
      }

  final private case class DecodeFailure(name: String, message: String) derives JsonDecoder

  private def json[A: JsonEncoder](status: Status, body: A): Response =
    Response.json(body.toJson).status(status)
