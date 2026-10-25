package com.github.olehbohatyi.mep.stores

import zio.test.*

import com.github.olehbohatyi.mep.domain.GeoPoint

object BoundingBoxSpec extends ZIOSpecDefault:

  def spec = suite("BoundingBox")(
    test("widens in longitude at high latitude") {
      val equator = BoundingBox.around(GeoPoint(0, 0), 10)
      val north   = BoundingBox.around(GeoPoint(60, 0), 10)
      assertTrue(
        (north.maxLon - north.minLon) > 1.9 * (equator.maxLon - equator.minLon),
        math.abs((north.maxLat - north.minLat) - (equator.maxLat - equator.minLat)) < 1e-9,
      )
    },
    test("contains points on the circle") {
      val center = GeoPoint(60, 10)
      val box    = BoundingBox.around(center, 10)
      // ~10 km due east
      val east   = GeoPoint(60, 10 + 10 / (111.32 * math.cos(math.toRadians(60))) * 0.99)
      assertTrue(east.lon <= box.maxLon, GeoPoint.distanceKm(center, east) <= 10.0)
    },
    test("is clamped at the poles") {
      val box = BoundingBox.around(GeoPoint(89.99, 0), 50)
      assertTrue(box.maxLat == 90.0)
    },
  )
