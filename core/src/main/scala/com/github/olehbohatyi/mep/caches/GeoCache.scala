package com.github.olehbohatyi.mep.caches

import zio.*
import zio.redis.*

import com.github.olehbohatyi.mep.domain.GeoPoint

// Caches the region resolved for a point, keyed by coordinates rounded to ~11 m
trait GeoCache:
  def get(point: GeoPoint): Task[Option[String]]
  def put(point: GeoPoint, region: String, ttl: Duration): Task[Unit]

object GeoCache:

  def key(point: GeoPoint): String = f"mep:geo:${point.lat}%.4f:${point.lon}%.4f"

  val live: URLayer[Redis, GeoCache] =
    ZLayer.fromFunction(GeoCacheLive(_))

  final private class GeoCacheLive(redis: Redis) extends GeoCache:

    def get(point: GeoPoint): Task[Option[String]] =
      redis.get(key(point)).returning[String]

    def put(point: GeoPoint, region: String, ttl: Duration): Task[Unit] =
      redis.set(key(point), region, expireTime = Some(ttl)).unit
