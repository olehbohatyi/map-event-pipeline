package com.github.olehbohatyi.mep.config

import zio.*
import zio.test.*

import com.github.olehbohatyi.mep.testkit.TestKafka

object AppConfigSpec extends ZIOSpecDefault:

  def spec = suite("AppConfig")(
    test("loads the mep block of application.conf") {
      for cfg <- ZIO.service[AppConfig]
      yield assertTrue(
        cfg.http.port == 8080,
        cfg.http.rateLimit == RateLimitConfig(600, 60),
        // Keys are required by default, and none ship with the code
        cfg.http.auth == AuthConfig(required = true, keys = ""),
        cfg.kafka.topics.raw == "map.events.raw",
        cfg.kafka.consumerGroup == "mep-pipe",
        cfg.kafka.topics.dlq == "map.events.dlq",
        cfg.kafka.topicSettings == TopicSettings(3, 1, 168, 336),
        cfg.postgres.maxPool == 10,
        // A broker or Postgres that stops answering fails a call within these instead of hanging it
        cfg.kafka.deliveryTimeout == 10.seconds,
        cfg.kafka.linger == 5.millis,
        cfg.postgres.connectTimeout == 5.seconds,
        cfg.postgres.socketTimeout == 30.seconds,
        cfg.kafka.topics.status == "map.events.status",
        cfg.kafka.topics.closed == "map.events.closed",
        cfg.lifecycle == LifecycleConfig(6.hours, 2.hours, 30.days, 1.minute),
      )
    },
    test("splits comma-separated bootstrap servers") {
      val cfg = TestKafka.config(servers = " a:9092, b:9092 ,")
      assertTrue(cfg.servers == List("a:9092", "b:9092"))
    },
  ).provide(AppConfig.live)
