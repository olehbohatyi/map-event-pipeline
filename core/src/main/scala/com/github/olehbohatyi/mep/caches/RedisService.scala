package com.github.olehbohatyi.mep.caches

import zio.*
import zio.redis.{ CodecSupplier, Redis, RedisError }

import com.github.olehbohatyi.mep.config.RedisConfig

object RedisService:

  /** zio-redis waits for an answer as long as it takes, so a Redis that stops answering (a partition, a frozen node)
    * hung every caller: the pipe sat on its partitions instead of failing and being restarted. The caches bound each
    * command by this; the rate limiter has its own, shorter bound.
    */
  val CommandTimeout: Duration = 2.seconds

  extension [A](command: Task[A])
    def bounded: Task[A] =
      command.timeoutFail(RedisTimeout(CommandTimeout))(CommandTimeout)

  final case class RedisTimeout(after: Duration) extends RuntimeException(s"Redis did not answer within $after")

  val live: ZLayer[RedisConfig, RedisError.IOError, Redis] =
    val redisConfig = ZLayer.fromFunction((c: RedisConfig) => zio.redis.RedisConfig(c.host, c.port))
    val codec       = ZLayer.succeed[CodecSupplier](CodecSupplier.utf8)
    (redisConfig ++ codec) >>> Redis.singleNode
