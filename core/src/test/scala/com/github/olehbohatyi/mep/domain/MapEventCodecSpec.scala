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
    // Events stored before the lifecycle, and open ones, have no closedAt: absent on the wire, as before
    test("omits closedAt while the event is open, and keeps it once closed") {
      val open   = Samples.event()
      val closed =
        open.copy(status = EventStatus.Resolved, closedAt = Some(java.time.Instant.parse("2026-11-20T12:00:00Z")))
      assertTrue(
        !open.toJson.contains("closedAt"),
        closed.toJson.contains("\"closedAt\":\"2026-11-20T12:00:00Z\""),
        closed.toJson.fromJson[MapEvent] == Right(closed),
      )
    },
    test("tells closed statuses from open ones") {
      assertTrue(
        EventStatus.values.filter(_.isClosed).toSet == Set(EventStatus.Resolved, EventStatus.Expired)
      )
    },
    test("rejects malformed payloads") {
      assertTrue("not json".fromJson[MapEvent].isLeft, "{}".fromJson[MapEvent].isLeft)
    },
  )
