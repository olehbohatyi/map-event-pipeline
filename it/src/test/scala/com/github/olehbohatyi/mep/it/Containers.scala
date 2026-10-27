package com.github.olehbohatyi.mep.it

import zio.*
import zio.kafka.admin.{ AdminClient, AdminClientSettings }

import com.github.olehbohatyi.mep.config.*
import org.testcontainers.containers.GenericContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import com.github.olehbohatyi.mep.testkit.TestKafka

// Throwaway infrastructure for integration tests (same images as docker-compose)
object Containers:

  // Same image, same digest as docker-compose; PostGIS images are not named "postgres", so declare the compatibility
  private val PostgisImage =
    DockerImageName
      .parse("postgis/postgis:18-3.6-alpine@sha256:c53a483fc274072dfa02cd491efa79f3c8ae0a10774a0ece6c9c53b6a632cdef")
      .asCompatibleSubstituteFor("postgres")

  final class RedisContainer extends GenericContainer[RedisContainer]("redis:8.6.3-alpine3.23")

  private def started[C <: GenericContainer[?]](make: => C): ZIO[Scope, Throwable, C] =
    ZIO.acquireRelease(ZIO.attemptBlocking { val c = make; c.start(); c })(c => ZIO.attemptBlocking(c.stop()).orDie)

  val postgres: ZLayer[Any, Throwable, PostgresConfig] =
    ZLayer.scoped(
      started(PostgreSQLContainer(PostgisImage))
        .map(c => PostgresConfig(c.getJdbcUrl, c.getUsername, c.getPassword, maxPool = 4))
    )

  val redis: ZLayer[Any, Throwable, RedisConfig] =
    ZLayer.scoped(
      started(RedisContainer().withExposedPorts(6379)).map(c => RedisConfig(c.getHost, c.getMappedPort(6379)))
    )

  // Auto-create is off, as in docker-compose: topics must come from TopicBootstrap
  val kafka: ZLayer[Any, Throwable, KafkaConfig] =
    ZLayer.scoped(
      for
        c  <- started(KafkaContainer("apache/kafka:4.3.0").withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"))
        cfg = TestKafka.config(servers = c.getBootstrapServers, group = "mep-it")
        _  <- untilReachable(cfg)
      yield cfg
    )

  // On CI the mapped port can refuse connections for a moment after the container reports it started. zio-kafka
  // checks the connection whenever it creates a client (producer, consumer or admin) and fails at once, with a
  // misleading "Failed to create new KafkaAdminClient", so a suite's shared Producer layer failed before any test ran.
  private def untilReachable(cfg: KafkaConfig): Task[Unit] =
    ZIO
      .scoped(AdminClient.make(AdminClientSettings(cfg.servers)).flatMap(_.listTopics()))
      .retry(Schedule.spaced(250.millis) && Schedule.recurs(240))
      .unit
      .withClock(Clock.ClockLive)
