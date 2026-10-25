package com.github.olehbohatyi.mep.geo

import zio.*
import zio.test.*

import com.github.olehbohatyi.mep.caches.GeoCache
import com.github.olehbohatyi.mep.domain.GeoPoint
import com.github.olehbohatyi.mep.testkit.InMemoryGeoCache

object RegionResolverSpec extends ZIOSpecDefault:

  def spec = suite("RegionResolver")(
    test("uses the 1-degree grid zone on a cache miss and caches it") {
      for
        resolver <- ZIO.service[RegionResolver]
        cache    <- ZIO.service[GeoCache]
        region   <- resolver.resolve(GeoPoint(50.45, 30.52))
        cached   <- cache.get(GeoPoint(50.45, 30.52))
      yield assertTrue(region == "zone_50_30", cached.contains("zone_50_30"))
    },
    test("prefers a cached region") {
      for
        resolver <- ZIO.service[RegionResolver]
        cache    <- ZIO.service[GeoCache]
        _        <- cache.put(GeoPoint(1, 2), "downtown", 1.hour)
        region   <- resolver.resolve(GeoPoint(1, 2))
      yield assertTrue(region == "downtown")
    },
    test("floors negative coordinates") {
      assertTrue(RegionResolver.zoneOf(GeoPoint(-0.5, -120.1)) == "zone_-1_-121")
    },
  ).provide(InMemoryGeoCache.layer, RegionResolver.live)
