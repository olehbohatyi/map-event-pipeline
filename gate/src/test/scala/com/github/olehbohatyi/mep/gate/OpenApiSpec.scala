package com.github.olehbohatyi.mep.gate

import zio.*
import zio.json.*
import zio.json.ast.Json
import zio.test.*

import java.nio.file.{ Files, Path }

/** Contract test: the generated OpenAPI document must equal `src/test/resources/openapi.json`, so any change to the API
  * surface shows up as a reviewable diff. After an intended change regenerate the file with
  * `UPDATE_OPENAPI=1 sbt -batch --server "gate/testOnly *OpenApiSpec"`.
  */
object OpenApiSpec extends ZIOSpecDefault:

  // Written only when regenerating (sbt forks into gate/ or runs from the root); reads use the classpath so mill works
  private lazy val goldenFile =
    List(Path.of("src/test/resources"), Path.of("gate/src/test/resources"))
      .find(Files.isDirectory(_))
      .get
      .resolve("openapi.json")

  // Object keys come out in hash order; sort them so the comparison and the committed file are stable
  private def sorted(json: Json): Json =
    json match
      case Json.Obj(fields) => Json.Obj(Chunk.from(fields.toList.sortBy(_._1).map((k, v) => k -> sorted(v))))
      case Json.Arr(items)  => Json.Arr(items.map(sorted))
      case other            => other

  private val generated: Json =
    sorted(Json.decoder.decodeJson(GateApi.openApi.toJson).toOption.get)

  private val update = sys.env.contains("UPDATE_OPENAPI")

  private def refs(json: Json): List[String] =
    json match
      case Json.Obj(fields) =>
        fields.toList.flatMap {
          case ("$ref", Json.Str(ref)) => List(ref)
          case (_, value)              => refs(value)
        }
      case Json.Arr(items)  => items.toList.flatMap(refs)
      case _                => Nil

  def spec = suite("OpenAPI")(
    test("matches the committed document") {
      for
        _        <- ZIO.when(update)(ZIO.attemptBlocking(Files.writeString(goldenFile, generated.toJsonPretty + "\n")).unit)
        expected <- ZIO.attemptBlocking(String(getClass.getResourceAsStream("/openapi.json").readAllBytes()))
      yield assertTrue(update || Json.decoder.decodeJson(expected) == Right(generated))
    },
    test("documents every route") {
      val paths = generated.asObject.flatMap(_.get("paths")).flatMap(_.asObject).map(_.keys.toSet)
      assertTrue(
        paths.contains(
          Set(
            "/api/v1/health",
            "/api/v1/events",
            "/api/v1/events/nearby",
            "/api/v1/events/recent",
            "/api/v1/events/live",
            "/api/v1/stats",
          )
        )
      )
    },
    // Regression: the generator referred to a ServerSentEvent schema for the live stream without writing one
    test("every $ref points at a schema the document defines") {
      val defined =
        generated
          .asObject
          .flatMap(_.get("components"))
          .flatMap(_.asObject)
          .flatMap(_.get("schemas"))
          .flatMap(_.asObject)
      val names   = defined.map(_.keys.toSet).getOrElse(Set.empty)
      val missing = refs(generated).map(_.stripPrefix("#/components/schemas/")).filterNot(names).distinct
      assertTrue(refs(generated).nonEmpty, missing.isEmpty)
    },
  )
