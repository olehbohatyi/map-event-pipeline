package com.github.olehbohatyi.mep.geo

import zio.*

import com.github.olehbohatyi.mep.caches.GeoCache
import com.github.olehbohatyi.mep.domain.GeoPoint

// Resolves the region of a point: cache first, then a 1-degree grid zone
trait RegionResolver:
  def resolve(point: GeoPoint): Task[String]

object RegionResolver:

  private val CacheTtl = 6.hours

  def zoneOf(point: GeoPoint): String =
    s"zone_${math.floor(point.lat).toInt}_${math.floor(point.lon).toInt}"

  val live: URLayer[GeoCache, RegionResolver] =
    ZLayer.fromFunction(RegionResolverLive(_))

  final private class RegionResolverLive(cache: GeoCache) extends RegionResolver:

    def resolve(point: GeoPoint): Task[String] =
      cache.get(point).flatMap {
        case Some(region) => ZIO.succeed(region)
        case None         =>
          val region = zoneOf(point)
          cache.put(point, region, CacheTtl).as(region)
      }
