package com.github.olehbohatyi.mep.caches

import zio.*
import zio.redis.*

import com.github.olehbohatyi.mep.caches.RedisService.bounded

// Namespaced set of seen keys with TTL, backed by Redis strings
trait IdempotencyCache:
  // Atomically marks the key with `value` (SET NX); false when it was already marked
  def tryMark(key: String, ttl: Duration, value: String = IdempotencyCache.Marked): Task[Boolean]
  // The value a key was marked with
  def get(key: String): Task[Option[String]]
  def isMarked(key: String): Task[Boolean]
  def mark(key: String, ttl: Duration): Task[Unit]
  def unmark(key: String): Task[Unit]

object IdempotencyCache:

  // What mark and tryMark store unless told otherwise; keys from before values were used all hold it
  val Marked = "1"

  def apply(namespace: String, redis: Redis): IdempotencyCache = IdempotencyCacheLive(namespace, redis)

  def live(namespace: String): URLayer[Redis, IdempotencyCache] =
    ZLayer.fromFunction(apply(namespace, _))

  final private class IdempotencyCacheLive(namespace: String, redis: Redis) extends IdempotencyCache:

    private def k(key: String): String = s"$namespace:$key"

    def tryMark(key: String, ttl: Duration, value: String): Task[Boolean] =
      redis.set(k(key), value, expireTime = Some(ttl), update = Some(Update.SetNew)).bounded

    def get(key: String): Task[Option[String]] =
      redis.get(k(key)).returning[String].bounded

    def isMarked(key: String): Task[Boolean] =
      redis.exists(k(key)).map(_ > 0L).bounded

    def mark(key: String, ttl: Duration): Task[Unit] =
      redis.set(k(key), Marked, expireTime = Some(ttl)).unit.bounded

    def unmark(key: String): Task[Unit] =
      redis.del(k(key)).unit.bounded
