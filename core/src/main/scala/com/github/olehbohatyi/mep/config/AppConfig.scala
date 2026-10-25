package com.github.olehbohatyi.mep.config

import zio.*
import zio.config.*
import zio.config.magnolia.*
import zio.config.typesafe.TypesafeConfigProvider

final case class AppConfig(
    http: HttpConfig,
    kafka: KafkaConfig,
    postgres: PostgresConfig,
    redis: RedisConfig,
  )

object AppConfig:

  private val descriptor: Config[AppConfig] =
    deriveConfig[AppConfig].mapKey(toKebabCase)

  val live: ZLayer[Any, Config.Error, AppConfig] =
    ZLayer.fromZIO(TypesafeConfigProvider.fromResourcePath().load(descriptor))

  // Narrow sub-layers so services depend only on what they use
  val http: URLayer[AppConfig, HttpConfig]         = ZLayer.fromFunction((c: AppConfig) => c.http)
  val kafka: URLayer[AppConfig, KafkaConfig]       = ZLayer.fromFunction((c: AppConfig) => c.kafka)
  val postgres: URLayer[AppConfig, PostgresConfig] = ZLayer.fromFunction((c: AppConfig) => c.postgres)
  val redis: URLayer[AppConfig, RedisConfig]       = ZLayer.fromFunction((c: AppConfig) => c.redis)

  val all: ZLayer[Any, Config.Error, HttpConfig & KafkaConfig & PostgresConfig & RedisConfig] =
    live >>> (http ++ kafka ++ postgres ++ redis)
