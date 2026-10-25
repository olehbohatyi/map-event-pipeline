package com.github.olehbohatyi.mep.domain

import zio.test.*

import com.github.olehbohatyi.mep.testkit.Samples.*

object GeoPointSpec extends ZIOSpecDefault:

  def spec = suite("GeoPoint")(
    test("haversine distance Kyiv–Lviv is ~468 km") {
      val d = GeoPoint.distanceKm(Kyiv, Lviv)
      assertTrue(d > 465.0, d < 471.0)
    },
    test("distance to itself is zero") {
      assertTrue(GeoPoint.distanceKm(Kyiv, Kyiv) == 0.0)
    },
  )
