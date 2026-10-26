package com.github.olehbohatyi.mep.geo

import zio.*

import com.github.olehbohatyi.mep.caches.GeoCache
import com.github.olehbohatyi.mep.domain.GeoPoint
import com.github.olehbohatyi.mep.stores.RegionRepo

// Resolves the region of a point: cache, then the region polygons in PostGIS, then a 1-degree grid zone for
// points outside every known region (other countries, open water)
trait RegionResolver:
  def resolve(point: GeoPoint): Task[String]

object RegionResolver:

  private val CacheTtl = 6.hours

  def zoneOf(point: GeoPoint): String =
    s"zone_${math.floor(point.lat).toInt}_${math.floor(point.lon).toInt}"

  val live: URLayer[GeoCache & RegionRepo, RegionResolver] =
    ZLayer.fromFunction(RegionResolverLive(_, _))

  final private class RegionResolverLive(cache: GeoCache, regions: RegionRepo) extends RegionResolver:

    def resolve(point: GeoPoint): Task[String] =
      cache
        .get(point)
        .someOrElseZIO(
          for
            found <- regions.findContaining(point)
            region = found.fold(zoneOf(point))(_.name)
            _     <- cache.put(point, region, CacheTtl)
          yield region
        )
