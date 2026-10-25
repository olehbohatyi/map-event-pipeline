package com.github.olehbohatyi.mep.testkit

import zio.*

import com.github.olehbohatyi.mep.caches.{ GeoCache, IdempotencyCache }
import com.github.olehbohatyi.mep.domain.GeoPoint

// In-memory cache doubles for unit tests (TTLs are ignored)

final class InMemoryIdempotencyCache(keys: Ref[Set[String]]) extends IdempotencyCache:
  def tryMark(key: String, ttl: Duration): Task[Boolean] = keys.modify(s => (!s(key), s + key))
  def isMarked(key: String): Task[Boolean]               = keys.get.map(_(key))
  def mark(key: String, ttl: Duration): Task[Unit]       = keys.update(_ + key)
  def unmark(key: String): Task[Unit]                    = keys.update(_ - key)
  def all: UIO[Set[String]]                              = keys.get

object InMemoryIdempotencyCache:
  val layer: ULayer[InMemoryIdempotencyCache] = ZLayer(Ref.make(Set.empty[String]).map(InMemoryIdempotencyCache(_)))

final class InMemoryGeoCache(entries: Ref[Map[String, String]]) extends GeoCache:
  def get(point: GeoPoint): Task[Option[String]]                      = entries.get.map(_.get(GeoCache.key(point)))
  def put(point: GeoPoint, region: String, ttl: Duration): Task[Unit] =
    entries.update(_ + (GeoCache.key(point) -> region))

object InMemoryGeoCache:
  val layer: ULayer[InMemoryGeoCache] = ZLayer(Ref.make(Map.empty[String, String]).map(InMemoryGeoCache(_)))
