package com.github.olehbohatyi.mep.domain

import zio.json.*
import zio.test.*

import com.github.olehbohatyi.mep.testkit.Samples

object MapEventCodecSpec extends ZIOSpecDefault:

  def spec = suite("MapEvent JSON")(
    test("round-trips") {
      val event = Samples.event(region = Some("zone_50_30"))
      assertTrue(event.toJson.fromJson[MapEvent] == Right(event))
    },
    test("encodes enums as their case names") {
      val json = Samples.event(severity = Severity.Critical).toJson
      assertTrue(json.contains("\"severity\":\"Critical\""), json.contains("\"eventType\":\"Hazard\""))
    },
    test("rejects unknown enum values") {
      val json = Samples.event().toJson.replace("\"Hazard\"", "\"Meteor\"")
      assertTrue(json.fromJson[MapEvent].isLeft)
    },
    test("rejects malformed payloads") {
      assertTrue("not json".fromJson[MapEvent].isLeft, "{}".fromJson[MapEvent].isLeft)
    },
  )
