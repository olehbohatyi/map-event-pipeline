package com.github.olehbohatyi.mep.gate

import zio.*
import zio.http.*
import zio.http.codec.*
import zio.http.endpoint.*
import zio.http.endpoint.openapi.{ JsonSchema, OpenAPI, OpenAPIGen }
import zio.http.endpoint.openapi.JsonSchema.{ SchemaRef, SchemaSpec, SchemaStyle }
import zio.json.{ JsonCodec, JsonDecoder, JsonEncoder }
import zio.schema.Schema
import zio.schema.codec.{ BinaryCodec, DecodeError }
import zio.stream.{ ZPipeline, ZSink }

import java.nio.charset.StandardCharsets.UTF_8

import com.github.olehbohatyi.mep.domain.MapEvent

/** The HTTP contract of the gate: the endpoints are the single source for both the routes and the OpenAPI document.
  */
object GateApi:

  /** JSON content codec that encodes with zio-json (the wire format stays exactly what clients already get, e.g. absent
    * `None` fields) while the zio-schema `Schema` feeds the OpenAPI generator.
    */
  private def jsonContent[A](using Schema[A], JsonEncoder[A], JsonDecoder[A]): HttpContentCodec[A] =
    val binary = new BinaryCodec[A]:
      def encode(value: A): Chunk[Byte] = Chunk.fromArray(JsonEncoder[A].encodeJson(value).toString.getBytes(UTF_8))

      def decode(whole: Chunk[Byte]): Either[DecodeError, A] =
        JsonDecoder[A].decodeJson(String(whole.toArray, UTF_8)).left.map(DecodeError.ReadError(Cause.empty, _))

      def streamEncoder: ZPipeline[Any, Nothing, A, Byte] = ZPipeline.mapChunks[A, Byte](_.flatMap(encode))

      def streamDecoder: ZPipeline[Any, DecodeError, Byte, A] =
        ZPipeline.fromSink(ZSink.collectAll[Byte].mapZIO(bytes => ZIO.fromEither(decode(bytes))))
    HttpContentCodec.from(MediaType.application.json -> BinaryCodecWithSchema(binary, Schema[A]))

  private given [A <: Product: {Schema, JsonCodec}]: HttpContentCodec[A] = jsonContent[A]

  private val IdempotencyKey = HeaderCodec.headerAs[String]("Idempotency-Key").optional

  /** zio-http's own decode failures (unreadable JSON, unknown enum value, missing or malformed query parameter) as
    * `{"error": "invalid request: ..."}`. Its default renders them by the client's Accept header, i.e. as an HTML page
    * to browsers; this codec is JSON-only. It replaces the default rather than going in front of it (`outCodecError`),
    * since with `Accept: text/html` the HTML-capable default would still be chosen.
    */
  private[gate] val invalidRequest: HttpCodec[HttpCodecType.ResponseType, HttpCodecError] =
    (HttpCodec.content[ApiError.BadRequest] ++ StatusCodec.BadRequest)
      .transform[HttpCodecError](bad => HttpCodecError.CustomError("InvalidRequest", bad.error))(err =>
        ApiError.BadRequest(s"invalid request: ${err.getMessage}")
      )

  extension [P, I, E, O, A <: AuthType](endpoint: Endpoint[P, I, E, O, A])
    private def invalidRequestAsJson: Endpoint[P, I, E, O, A] = endpoint.copy(codecError = invalidRequest)

  val health =
    Endpoint(Method.GET / "api" / "v1" / "health")
      .out[String](MediaType.text.plain, Doc.p("Liveness probe; always `OK`."))
      .invalidRequestAsJson

  val ingest =
    Endpoint(Method.POST / "api" / "v1" / "events")
      .header(IdempotencyKey)
      .in[EventRequest](Doc.p("The road event to publish."))
      .out[IngestAccepted](Status.Accepted, Doc.p("The event was published to Kafka."))
      .out[IngestDuplicate](Status.Ok, Doc.p("The Idempotency-Key was seen before; nothing was published."))
      .invalidRequestAsJson
      .outErrors[ApiError](
        HttpCodec.error[ApiError.BadRequest](Status.BadRequest),
        HttpCodec.error[ApiError.Conflict](Status.Conflict) ?? Doc.p(
          "Another request with this Idempotency-Key is still being published, or has just failed to be. Nothing is " +
            "known to be published yet: retry the request."
        ),
        // Answered by GateRoutes.guarded before the request is read; declared here for the document
        HttpCodec.error[ApiError.Unauthorized](Status.Unauthorized) ?? Doc.p(
          "No API key, or one the gate does not know. Send `Authorization: Bearer <key>`."
        ),
        // Answered by GateRoutes.guarded before the request is read; declared here for the document
        HttpCodec.error[ApiError.TooManyRequests](Status.TooManyRequests) ?? Doc.p(
          "Too many events from this client (its API key, or its address when no key is required) in the current " +
            "window. `Retry-After` gives the seconds until the next window."
        ),
        HttpCodec.error[ApiError.Internal](Status.InternalServerError),
      )

  val resolve =
    Endpoint(Method.POST / "api" / "v1" / "events" / PathCodec.uuid("id") / "resolve")
      .out[ResolveAccepted](
        Status.Accepted,
        Doc.p(
          "The request was published; the pipe marks the event `Resolved` shortly. It then leaves the nearby and recent " +
            "lists and the live map, which receives it once more with its new status."
        ),
      )
      .invalidRequestAsJson
      .outErrors[ApiError](
        HttpCodec.error[ApiError.NotFound](Status.NotFound) ?? Doc.p("No stored event with this id (yet)."),
        HttpCodec.error[ApiError.Conflict](Status.Conflict) ?? Doc
          .p("The event is closed already: resolved or expired."),
        // Answered by GateRoutes.guarded before the request is read; declared here for the document
        HttpCodec.error[ApiError.Unauthorized](Status.Unauthorized) ?? Doc.p(
          "No API key, or one the gate does not know. Send `Authorization: Bearer <key>`."
        ),
        // Answered by GateRoutes.guarded before the request is read; declared here for the document
        HttpCodec.error[ApiError.TooManyRequests](Status.TooManyRequests) ?? Doc.p(
          "Too many submissions from this client in the current window (shared with event submissions)."
        ),
        HttpCodec.error[ApiError.Internal](Status.InternalServerError),
      )

  val nearby =
    Endpoint(Method.GET / "api" / "v1" / "events" / "nearby")
      .query(QueryCodec.query[Double]("lat") ?? Doc.p("Latitude of the centre, within [-90, 90]."))
      .query(QueryCodec.query[Double]("lon") ?? Doc.p("Longitude of the centre, within [-180, 180]."))
      .query(QueryCodec.query[Double]("radius").optional ?? Doc.p("Radius in km, within (0, 100]; default 5."))
      .query(QueryCodec.query[Int]("limit").optional ?? Doc.p("Maximum number of events, within [1, 500]; default 50."))
      .out[NearbyResponse](Doc.p("Open events around the point, closest first, and the region the point is in."))
      .invalidRequestAsJson
      .outErrors[ApiError](
        HttpCodec.error[ApiError.BadRequest](Status.BadRequest),
        HttpCodec.error[ApiError.Internal](Status.InternalServerError),
      )

  // A primary-key read: lets a client (or the load test's probe) see when its event has been stored
  val event =
    Endpoint(Method.GET / "api" / "v1" / "events" / PathCodec.uuid("id"))
      .out[MapEvent](Doc.p("The event, once the pipe has stored it, open or closed."))
      .invalidRequestAsJson
      .outErrors[ApiError](
        HttpCodec.error[ApiError.NotFound](Status.NotFound),
        HttpCodec.error[ApiError.Internal](Status.InternalServerError),
      )

  val recent =
    Endpoint(Method.GET / "api" / "v1" / "events" / "recent")
      .query(
        QueryCodec.query[Int]("limit").optional ?? Doc.p("Maximum number of events, within [1, 500]; default 200.")
      )
      .out[RecentResponse](Doc.p("The latest open events by occurrence time, newest first."))
      .invalidRequestAsJson
      .outErrors[ApiError](
        HttpCodec.error[ApiError.BadRequest](Status.BadRequest),
        HttpCodec.error[ApiError.Internal](Status.InternalServerError),
      )

  val live =
    Endpoint(Method.GET / "api" / "v1" / "events" / "live")
      .query(
        QueryCodec.query[String]("bbox").optional ??
          Doc.p("Only events inside `minLon,minLat,maxLon,maxLat`; all events when absent.")
      )
      .outStream[ServerSentEvent[String]](
        Doc.p(
          "Server-sent events, one per event the pipe finishes or closes: `event: event`, `id: <event id>`, `data: " +
            "<MapEvent JSON>`. A closed event arrives again with status `Resolved` or `Expired`. A `:heartbeat` comment every 15 seconds keeps the connection open. Delivery is best effort: a " +
            "client that falls far behind loses its oldest events, and the same event may arrive twice."
        )
      )
      .invalidRequestAsJson
      .outError[ApiError.BadRequest](Status.BadRequest)

  val stats =
    Endpoint(Method.GET / "api" / "v1" / "stats")
      .query(QueryCodec.query[String]("region").optional ?? Doc.p("Only this region; all regions when absent."))
      .out[StatsResponse](Doc.p("Event counts per region and type, as aggregated by the stat job."))
      .invalidRequestAsJson
      .outError[ApiError.Internal](Status.InternalServerError)

  val hourly =
    Endpoint(Method.GET / "api" / "v1" / "stats" / "hourly")
      .query(QueryCodec.query[String]("region").optional ?? Doc.p("Only this region; all regions when absent."))
      .query(
        QueryCodec.query[Int]("hours").optional ??
          Doc.p("How many hours back, the current one included, within [1, 168]; default 24.")
      )
      .out[HourlyResponse](
        Doc.p(
          "Event counts per hour (UTC, by the gate's receive time), region and type, kept current by the streaming job."
        )
      )
      .invalidRequestAsJson
      .outErrors[ApiError](
        HttpCodec.error[ApiError.BadRequest](Status.BadRequest),
        HttpCodec.error[ApiError.Internal](Status.InternalServerError),
      )

  val endpoints = List(health, ingest, resolve, nearby, event, recent, live, stats, hourly)

  // The name of the API key scheme in the OpenAPI document (before openApi, which reads it as the object initialises)
  val SecurityScheme = "apiKey"

  val openApi: OpenAPI =
    val generated = OpenAPIGen.fromEndpoints("Map Event Pipeline API", "1.0.0", endpoints.head, endpoints.tail*)
    // The generator refers to a ServerSentEvent schema for the live stream but never writes one
    val sse       = OpenAPI.Key.fromString("ServerSentEvent").get ->
      OpenAPI
        .ReferenceOr
        .Or(JsonSchema.fromZSchema(ServerSentEvent.schema[String], SchemaRef(SchemaSpec.OpenAPI, SchemaStyle.Inline)))
    val comps     = generated.components.getOrElse(OpenAPI.Components())
    // The key is checked by GateRoutes.guarded, not by the endpoints (an endpoint's own auth would demand a key even
    // when the gate is configured not to), so the document gets the scheme here. Every POST is a write that needs it
    val scheme    = OpenAPI.Key.fromString(SecurityScheme).get    -> OpenAPI
      .ReferenceOr
      .Or(
        OpenAPI
          .SecurityScheme
          .Http(Some(Doc.p("An API key from the gate's operator, as a bearer token.")), "bearer", None)
      )
    val keyed     = List(OpenAPI.SecurityScheme.SecurityRequirement(Map(SecurityScheme -> Nil)))
    val paths     = generated.paths.map((path, item) => path -> item.copy(post = item.post.map(_.copy(security = keyed))))
    generated.copy(
      paths = paths,
      components = Some(comps.copy(schemas = comps.schemas + sse, securitySchemes = comps.securitySchemes + scheme)),
    )
