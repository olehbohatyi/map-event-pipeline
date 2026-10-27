package com.github.olehbohatyi.mep.gate

import zio.*
import zio.http.*
import zio.http.codec.PathCodec
import zio.http.endpoint.openapi.SwaggerUI
import zio.json.*
import zio.stream.ZStream

import com.github.olehbohatyi.mep.domain.{ GeoPoint, MapEvent }

object GateRoutes:

  // POST /api/v1/events        — ingest a road event (optional Idempotency-Key header)
  // GET  /api/v1/events/nearby — events around lat/lon within radius (km)
  // GET  /api/v1/events/recent — the latest events, newest first
  // GET  /api/v1/events/live   — server-sent events as the pipe finishes them, optionally within ?bbox=
  // GET  /api/v1/stats         — aggregated counts, optionally ?region=
  // GET  /api/v1/health        — liveness probe
  // GET  /docs/openapi         — Swagger UI over the generated OpenAPI document (assets come from unpkg.com)
  // GET  /map                  — live map page (Leaflet and OpenStreetMap tiles load from the internet)

  private val MaxRadiusKm   = 100.0
  private val MaxLimit      = 500
  private val DefaultRecent = 200
  private val Heartbeat     = 15.seconds
  // How far ahead of the gate's clock a client's occurredAt may be (clock drift), before it is rejected
  private val MaxClockSkew  = 5.minutes

  def apply(ingestion: IngestionService, query: QueryService, feed: LiveFeed): Routes[Any, Nothing] =
    api(ingestion, query, feed) ++
      SwaggerUI.routes(PathCodec.path("docs") / "openapi", GateApi.openApi).handleError(identity) ++
      Routes(Method.GET / "map" -> mapPage)

  // Served from the jar, whose entries carry a fixed timestamp (and the ETag only that and the size), so browsers would
  // treat the page as fresh for years and keep an old version after a deploy. It is small: never cache it.
  private val mapPage: Handler[Any, Nothing, Request, Response] =
    Handler.fromResource("map/index.html").orDie.map(_.addHeader(Header.CacheControl.NoStore))

  // The API routes alone, without the docs and the map page
  def api(ingestion: IngestionService, query: QueryService, feed: LiveFeed): Routes[Any, Nothing] =
    Routes(
      GateApi.health.implement(_ => ZIO.succeed("OK")),
      GateApi.ingest.implement((key, req) => postEvent(ingestion, req, key)),
      GateApi.nearby.implement((lat, lon, radius, limit) => getNearby(query, lat, lon, radius, limit)),
      GateApi.recent.implement(limit => getRecent(query, limit)),
      GateApi.live.implement(bbox => liveEvents(feed, bbox)),
      GateApi.stats.implement(region => getStats(query, region)),
    ).transform(_.contramap[Request](readBodyAsJson))

  // Bodies are JSON whatever Content-Type the client sends (`curl -d` says form-urlencoded), as before the Endpoint API;
  // zio-http picks the body decoder by that media type, so set it before decoding
  private def readBodyAsJson(req: Request): Request =
    req.copy(body = req.body.contentType(MediaType.application.json))

  private def internal(op: String)(err: Throwable): UIO[ApiError.Internal] =
    ZIO.logErrorCause(s"$op failed", Cause.fail(err)).as(ApiError.Internal("internal"))

  private def postEvent(
      ingestion: IngestionService,
      req: EventRequest,
      idempotencyKey: Option[String],
    ): IO[ApiError, Either[IngestDuplicate, IngestAccepted]] =
    notInTheFuture(req) *>
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

  // occurredAt is client input. A far-future one would sort first in /recent for good and never age off the map
  private def notInTheFuture(req: EventRequest): IO[ApiError.BadRequest, Unit] =
    Clock.instant.flatMap { now =>
      ZIO
        .fail(ApiError.BadRequest(s"occurredAt must not be more than ${MaxClockSkew.toMinutes} minutes in the future"))
        .when(req.occurredAt.exists(_.isAfter(now.plus(MaxClockSkew))))
        .unit
    }

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

  private def getRecent(query: QueryService, limit: Option[Int]): IO[ApiError, RecentResponse] =
    val l = limit.getOrElse(DefaultRecent)
    if l <= 0 || l > MaxLimit then ZIO.fail(ApiError.BadRequest(s"limit must be within [1, $MaxLimit]"))
    else query.recent(l).catchAll(internalFail("recent query"))

  // The payload is the same zio-json MapEvent as everywhere else (absent fields omitted), and the id lets clients
  // drop the occasional duplicate. Heartbeats keep proxies from closing an idle connection.
  private def liveEvents(
      feed: LiveFeed,
      bbox: Option[String],
    ): IO[ApiError.BadRequest, ZStream[Any, Nothing, ServerSentEvent[String]]] =
    ZIO
      .fromEither(bbox.filter(_.nonEmpty).map(BoundingBox.parse).fold(Right(None))(_.map(Some(_))))
      .mapError(ApiError.BadRequest(_))
      .map { area =>
        feed
          .subscribe(area)
          .map(sse)
          .merge(ZStream.tick(Heartbeat).as(ServerSentEvent.heartbeat), ZStream.HaltStrategy.Left)
      }

  private def sse(event: MapEvent): ServerSentEvent[String] =
    ServerSentEvent(event.toJson, eventType = Some("event"), id = Some(event.id.toString))

  private def getStats(query: QueryService, region: Option[String]): IO[ApiError.Internal, StatsResponse] =
    val filter = region.filter(_.nonEmpty)
    query.stats(filter).map(StatsResponse(filter, _)).catchAll(internalFail("stats query"))

  private def internalFail(op: String)(err: Throwable): IO[ApiError.Internal, Nothing] =
    internal(op)(err).flatMap(ZIO.fail(_))
