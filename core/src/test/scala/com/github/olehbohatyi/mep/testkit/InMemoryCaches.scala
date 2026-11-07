package com.github.olehbohatyi.mep.testkit

import zio.*

import com.github.olehbohatyi.mep.caches.{ GeoCache, IdempotencyCache, RateLimiter }
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

// Allows `limit` requests per client, then limits it for good (windows are left to the Redis test); `fail` makes
// every call fail, as an unreachable Redis would, and `hang` makes it wait forever, as one that never answers
final class InMemoryRateLimiter(
    limit: Int,
    counts: Ref[Map[String, Int]],
    failing: Ref[Boolean],
    hanging: Ref[Boolean],
  ) extends RateLimiter:
  def acquire(client: String): Task[RateLimiter.Decision] =
    ZIO.whenZIO(hanging.get)(ZIO.never) *>
      ZIO.whenZIO(failing.get)(ZIO.fail(RuntimeException("redis down"))) *>
      counts
        .modify(m => (m.getOrElse(client, 0) + 1, m.updated(client, m.getOrElse(client, 0) + 1)))
        .map(n => if n <= limit then RateLimiter.Decision.Allowed else RateLimiter.Decision.Limited(42.seconds))
  def seen: UIO[Map[String, Int]]                         = counts.get
  def fail: UIO[Unit]                                     = failing.set(true)
  def hang: UIO[Unit]                                     = hanging.set(true)

object InMemoryRateLimiter:
  def make(limit: Int): UIO[InMemoryRateLimiter] =
    (Ref.make(Map.empty[String, Int]) <*> Ref.make(false) <*> Ref.make(false))
      .map((c, f, h) => InMemoryRateLimiter(limit, c, f, h))
