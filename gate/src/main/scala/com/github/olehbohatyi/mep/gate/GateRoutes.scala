package com.github.olehbohatyi.mep.gate

import zio.*
import zio.http.*
import zio.http.codec.PathCodec
import zio.http.endpoint.openapi.SwaggerUI
import zio.json.*
import zio.stream.ZStream
import zio.telemetry.opentelemetry.common.{ Attribute, Attributes }
import zio.telemetry.opentelemetry.context.IncomingContextCarrier
import zio.telemetry.opentelemetry.tracing.Tracing
import zio.telemetry.opentelemetry.tracing.propagation.TraceContextPropagator

import io.opentelemetry.api.trace.{ SpanKind, StatusCode }
import scala.collection.mutable
import com.github.olehbohatyi.mep.domain.{ GeoPoint, MapEvent }

object GateRoutes:

  // POST /api/v1/events        — ingest a road event (optional Idempotency-Key header)
  // GET  /api/v1/events/nearby — events around lat/lon within radius (km)
  // GET  /api/v1/events/recent — the latest events, newest first
  // GET  /api/v1/events/live   — server-sent events as the pipe finishes them, optionally within ?bbox=
  // GET  /api/v1/stats         — aggregated counts, optionally ?region=
  // GET  /api/v1/stats/hourly  — hourly counts from the streaming job, optionally ?region= and ?hours=
  // GET  /api/v1/health        — liveness probe
  // GET  /docs/openapi         — Swagger UI over the generated OpenAPI document (assets come from unpkg.com)
  // GET  /map                  — live map page (Leaflet and OpenStreetMap tiles load from the internet)

  private val MaxRadiusKm   = 100.0
  private val MaxLimit      = 500
  private val DefaultRecent = 200
  private val Heartbeat     = 15.seconds
  // How far ahead of the gate's clock a client's occurredAt may be (clock drift), before it is rejected
  private val MaxClockSkew  = 5.minutes
  private val DefaultHours  = 24
  private val MaxHours      = 168

  def apply(ingestion: IngestionService, query: QueryService, feed: LiveFeed, tracing: Tracing): Routes[Any, Nothing] =
    observed(tracing)(
      api(ingestion, query, feed) ++
        SwaggerUI.routes(PathCodec.path("docs") / "openapi", GateApi.openApi).handleError(identity) ++
        Routes(Method.GET / "map" -> mapPage) ++
        // Unknown paths answer 404 from a real route, so they are traced and counted too: zio-http's server merges the
        // app into its own (`++` keeps the left side's not-found handler), so a wrapped not-found handler is lost
        Routes(Method.ANY / trailing -> Handler.notFound)
    )

  /** A server span per request that continues the caller's `traceparent` (the producer span and the pipe's spans then
    * join the same trace), plus request count and latency. For the live stream both cover the response head only.
    */
  private def observed(tracing: Tracing)(routes: Routes[Any, Nothing]): Routes[Any, Nothing] =
    routes.transform { handle =>
      // The request's own scope, which zio-http opens per request and closes after the response (body included)
      Handler.scoped[Any](Handler.fromFunctionZIO[Request] { req =>
        val path    = req.path.encode
        val headers =
          mutable.Map.from(req.headers.toList.map(h => h.headerName.toString.toLowerCase -> h.renderedValue))
        tracing.extractSpan(
          TraceContextPropagator.default,
          IncomingContextCarrier.default(headers),
          s"${req.method} $path",
          SpanKind.SERVER,
          Attributes(Attribute.string("http.request.method", req.method.toString), Attribute.string("url.path", path)),
        ) {
          handle(req).merge.timed.flatMap { (took, res) =>
            val route = if res.status == Status.NotFound then "unmatched" else path
            tracing.setAttribute("http.response.status_code", res.status.code.toLong) *>
              tracing.getCurrentSpanUnsafe.map(_.setStatus(StatusCode.ERROR)).when(res.status.isServerError) *>
              GateMetrics.request(req.method.toString, route, res.status.code, took).as(res)
          }
        }
      })
    }

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
      GateApi.event.implement(id => getEvent(query, id)),
      GateApi.recent.implement(limit => getRecent(query, limit)),
      GateApi.live.implement(bbox => liveEvents(feed, bbox)),
      GateApi.stats.implement(region => getStats(query, region)),
      GateApi.hourly.implement((region, hours) => getHourly(query, region, hours)),
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
        .tap {
          case IngestionResult.Accepted(_)  => GateMetrics.ingest(req, "accepted")
          case IngestionResult.Duplicate(_) => GateMetrics.ingest(req, "duplicate")
        }
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

  private def getEvent(query: QueryService, id: java.util.UUID): IO[ApiError, MapEvent] =
    query
      .find(id)
      .catchAll(internalFail("event query"))
      .someOrFail(ApiError.NotFound(s"no stored event $id"))

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

  private def getHourly(query: QueryService, region: Option[String], hours: Option[Int]): IO[ApiError, HourlyResponse] =
    val filter = region.filter(_.nonEmpty)
    val h      = hours.getOrElse(DefaultHours)
    if h <= 0 || h > MaxHours then ZIO.fail(ApiError.BadRequest(s"hours must be within [1, $MaxHours]"))
    else query.hourly(filter, h).map(HourlyResponse(filter, h, _)).catchAll(internalFail("hourly query"))

  private def internalFail(op: String)(err: Throwable): IO[ApiError.Internal, Nothing] =
    internal(op)(err).flatMap(ZIO.fail(_))
