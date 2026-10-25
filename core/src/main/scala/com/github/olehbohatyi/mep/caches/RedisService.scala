package com.github.olehbohatyi.mep.caches

import zio.*
import zio.redis.{ CodecSupplier, Redis, RedisError }

import com.github.olehbohatyi.mep.config.RedisConfig

object RedisService:

  val live: ZLayer[RedisConfig, RedisError.IOError, Redis] =
    val redisConfig = ZLayer.fromFunction((c: RedisConfig) => zio.redis.RedisConfig(c.host, c.port))
    val codec       = ZLayer.succeed[CodecSupplier](CodecSupplier.utf8)
    (redisConfig ++ codec) >>> Redis.singleNode
