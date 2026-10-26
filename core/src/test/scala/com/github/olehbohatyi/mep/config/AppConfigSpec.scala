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
        cfg.kafka.topics.raw == "map.events.raw",
        cfg.kafka.consumerGroup == "mep-pipe",
        cfg.kafka.topics.dlq == "map.events.dlq",
        cfg.kafka.topicSettings == TopicSettings(3, 1, 168, 336),
        cfg.postgres.maxPool == 10,
      )
    },
    test("splits comma-separated bootstrap servers") {
      val cfg = TestKafka.config(servers = " a:9092, b:9092 ,")
      assertTrue(cfg.servers == List("a:9092", "b:9092"))
    },
  ).provide(AppConfig.live)
