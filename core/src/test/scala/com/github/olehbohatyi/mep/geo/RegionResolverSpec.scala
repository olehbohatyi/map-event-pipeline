package com.github.olehbohatyi.mep.geo

import zio.*
import zio.test.*

import com.github.olehbohatyi.mep.caches.GeoCache
import com.github.olehbohatyi.mep.domain.GeoPoint
import com.github.olehbohatyi.mep.testkit.{ InMemoryGeoCache, InMemoryRegionRepo, Samples }

object RegionResolverSpec extends ZIOSpecDefault:

  def spec = suite("RegionResolver")(
    test("resolves the region polygon containing the point and caches it") {
      for
        resolver <- ZIO.service[RegionResolver]
        cache    <- ZIO.service[GeoCache]
        repo     <- ZIO.service[InMemoryRegionRepo]
        first    <- resolver.resolve(Samples.Lviv)
        second   <- resolver.resolve(Samples.Lviv)
        cached   <- cache.get(Samples.Lviv)
        lookups  <- repo.lookupCount
      yield assertTrue(first == "Lviv Oblast", second == "Lviv Oblast", cached.contains("Lviv Oblast"), lookups == 1)
    },
    test("falls back to the 1-degree grid zone outside every region, and caches that too") {
      for
        resolver <- ZIO.service[RegionResolver]
        cache    <- ZIO.service[GeoCache]
        repo     <- ZIO.service[InMemoryRegionRepo]
        region   <- resolver.resolve(GeoPoint(50.45, 30.52))
        _        <- resolver.resolve(GeoPoint(50.45, 30.52))
        cached   <- cache.get(GeoPoint(50.45, 30.52))
        lookups  <- repo.lookupCount
      yield assertTrue(region == "zone_50_30", cached.contains("zone_50_30"), lookups == 1)
    },
    test("prefers a cached region without asking the database") {
      for
        resolver <- ZIO.service[RegionResolver]
        cache    <- ZIO.service[GeoCache]
        repo     <- ZIO.service[InMemoryRegionRepo]
        _        <- cache.put(GeoPoint(1, 2), "downtown", 1.hour)
        region   <- resolver.resolve(GeoPoint(1, 2))
        lookups  <- repo.lookupCount
      yield assertTrue(region == "downtown", lookups == 0)
    },
    test("floors negative coordinates") {
      assertTrue(RegionResolver.zoneOf(GeoPoint(-0.5, -120.1)) == "zone_-1_-121")
    },
  ).provide(InMemoryGeoCache.layer, InMemoryRegionRepo.layer(Samples.LvivOblast), RegionResolver.live)
