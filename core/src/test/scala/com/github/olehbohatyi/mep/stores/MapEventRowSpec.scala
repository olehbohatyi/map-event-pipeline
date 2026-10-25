package com.github.olehbohatyi.mep.stores

import zio.test.*

import com.github.olehbohatyi.mep.testkit.Samples

object MapEventRowSpec extends ZIOSpecDefault:

  def spec = suite("MapEventRow")(
    test("maps to and from the domain model") {
      val event = Samples.event(region = Some("zone_50_30"))
      val row   = MapEventRow.from(event)
      assertTrue(row.lat == event.location.lat, row.lon == event.location.lon, MapEventRow.toDomain(row) == event)
    }
  )
