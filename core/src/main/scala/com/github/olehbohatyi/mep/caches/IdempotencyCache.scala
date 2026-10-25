package com.github.olehbohatyi.mep.caches

import zio.*
import zio.redis.*

// Namespaced set of seen keys with TTL, backed by Redis strings
trait IdempotencyCache:
  // Atomically marks the key (SET NX); false when it was already marked
  def tryMark(key: String, ttl: Duration): Task[Boolean]
  def isMarked(key: String): Task[Boolean]
  def mark(key: String, ttl: Duration): Task[Unit]
  def unmark(key: String): Task[Unit]

object IdempotencyCache:

  def apply(namespace: String, redis: Redis): IdempotencyCache = IdempotencyCacheLive(namespace, redis)

  def live(namespace: String): URLayer[Redis, IdempotencyCache] =
    ZLayer.fromFunction(apply(namespace, _))

  final private class IdempotencyCacheLive(namespace: String, redis: Redis) extends IdempotencyCache:

    private def k(key: String): String = s"$namespace:$key"

    def tryMark(key: String, ttl: Duration): Task[Boolean] =
      redis.set(k(key), "1", expireTime = Some(ttl), update = Some(Update.SetNew))

    def isMarked(key: String): Task[Boolean] =
      redis.exists(k(key)).map(_ > 0L)

    def mark(key: String, ttl: Duration): Task[Unit] =
      redis.set(k(key), "1", expireTime = Some(ttl)).unit

    def unmark(key: String): Task[Unit] =
      redis.del(k(key)).unit
