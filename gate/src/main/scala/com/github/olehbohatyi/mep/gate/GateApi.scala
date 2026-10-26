package com.github.olehbohatyi.mep.gate

import zio.*
import zio.http.*
import zio.http.codec.*
import zio.http.endpoint.*
import zio.http.endpoint.openapi.{ OpenAPI, OpenAPIGen }
import zio.json.{ JsonCodec, JsonDecoder, JsonEncoder }
import zio.schema.Schema
import zio.schema.codec.{ BinaryCodec, DecodeError }
import zio.stream.{ ZPipeline, ZSink }

import java.nio.charset.StandardCharsets.UTF_8

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

  val health =
    Endpoint(Method.GET / "api" / "v1" / "health")
      .out[String](MediaType.text.plain, Doc.p("Liveness probe; always `OK`."))

  val ingest =
    Endpoint(Method.POST / "api" / "v1" / "events")
      .header(IdempotencyKey)
      .in[EventRequest](Doc.p("The road event to publish."))
      .out[IngestAccepted](Status.Accepted, Doc.p("The event was published to Kafka."))
      .out[IngestDuplicate](Status.Ok, Doc.p("The Idempotency-Key was seen before; nothing was published."))
      .outErrors[ApiError](
        HttpCodec.error[ApiError.BadRequest](Status.BadRequest),
        HttpCodec.error[ApiError.Internal](Status.InternalServerError),
      )

  val nearby =
    Endpoint(Method.GET / "api" / "v1" / "events" / "nearby")
      .query(QueryCodec.query[Double]("lat") ?? Doc.p("Latitude of the centre, within [-90, 90]."))
      .query(QueryCodec.query[Double]("lon") ?? Doc.p("Longitude of the centre, within [-180, 180]."))
      .query(QueryCodec.query[Double]("radius").optional ?? Doc.p("Radius in km, within (0, 100]; default 5."))
      .query(QueryCodec.query[Int]("limit").optional ?? Doc.p("Maximum number of events, within [1, 500]; default 50."))
      .out[NearbyResponse](Doc.p("Events around the point, closest first, and the region the point is in."))
      .outErrors[ApiError](
        HttpCodec.error[ApiError.BadRequest](Status.BadRequest),
        HttpCodec.error[ApiError.Internal](Status.InternalServerError),
      )

  val stats =
    Endpoint(Method.GET / "api" / "v1" / "stats")
      .query(QueryCodec.query[String]("region").optional ?? Doc.p("Only this region; all regions when absent."))
      .out[StatsResponse](Doc.p("Event counts per region and type, as aggregated by the stat job."))
      .outError[ApiError.Internal](Status.InternalServerError)

  val endpoints = List(health, ingest, nearby, stats)

  val openApi: OpenAPI = OpenAPIGen.fromEndpoints(
    "Map Event Pipeline API",
    "1.0.0",
    health,
    ingest,
    nearby,
    stats,
  )
