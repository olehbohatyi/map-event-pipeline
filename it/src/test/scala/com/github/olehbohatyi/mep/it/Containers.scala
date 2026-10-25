package com.github.olehbohatyi.mep.it

import zio.*

import com.github.olehbohatyi.mep.config.*
import org.testcontainers.containers.GenericContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.postgresql.PostgreSQLContainer

// Throwaway infrastructure for integration tests (same images as docker-compose)
object Containers:

  final class RedisContainer extends GenericContainer[RedisContainer]("redis:8.6.3-alpine3.23")

  private def started[C <: GenericContainer[?]](make: => C): ZIO[Scope, Throwable, C] =
    ZIO.acquireRelease(ZIO.attemptBlocking { val c = make; c.start(); c })(c => ZIO.attemptBlocking(c.stop()).orDie)

  val postgres: ZLayer[Any, Throwable, PostgresConfig] =
    ZLayer.scoped(
      started(PostgreSQLContainer("postgres:18.4-alpine3.23"))
        .map(c => PostgresConfig(c.getJdbcUrl, c.getUsername, c.getPassword, maxPool = 4))
    )

  val redis: ZLayer[Any, Throwable, RedisConfig] =
    ZLayer.scoped(
      started(RedisContainer().withExposedPorts(6379)).map(c => RedisConfig(c.getHost, c.getMappedPort(6379)))
    )

  val topics: TopicsConfig = TopicsConfig("raw", "enriched", "hazards", "signs", "traffic")

  val kafka: ZLayer[Any, Throwable, KafkaConfig] =
    ZLayer.scoped(
      started(KafkaContainer("apache/kafka:4.3.0")).map(c => KafkaConfig(c.getBootstrapServers, "mep-it", topics))
    )
