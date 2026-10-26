package com.github.olehbohatyi.mep.caches

import zio.*
import zio.redis.*

import com.github.olehbohatyi.mep.domain.GeoPoint

// Caches the region resolved for a point, keyed by coordinates rounded to ~11 m.
//
// Cached values outlive deployments (Redis keeps its data), so a value written by an older resolver would
// otherwise keep being served until its TTL runs out. The key carries a version: bump `KeyVersion` whenever
// the way regions are resolved or named changes, and old entries are simply never read again.
trait GeoCache:
  def get(point: GeoPoint): Task[Option[String]]
  def put(point: GeoPoint, region: String, ttl: Duration): Task[Unit]

object GeoCache:

  // v1: 1-degree grid zones (zone_50_30)   v2: region polygons from PostGIS
  val KeyVersion = "v2"

  def key(point: GeoPoint): String = f"mep:geo:$KeyVersion:${point.lat}%.4f:${point.lon}%.4f"

  val live: URLayer[Redis, GeoCache] =
    ZLayer.fromFunction(GeoCacheLive(_))

  final private class GeoCacheLive(redis: Redis) extends GeoCache:

    def get(point: GeoPoint): Task[Option[String]] =
      redis.get(key(point)).returning[String]

    def put(point: GeoPoint, region: String, ttl: Duration): Task[Unit] =
      redis.set(key(point), region, expireTime = Some(ttl)).unit
