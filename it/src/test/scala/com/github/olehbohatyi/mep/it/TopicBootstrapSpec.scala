package com.github.olehbohatyi.mep.it

import zio.*
import zio.kafka.admin.{ AdminClient, AdminClientSettings }
import zio.kafka.admin.AdminClient.{ ConfigResource, ConfigResourceType }
import zio.test.*

import com.github.olehbohatyi.mep.broker.TopicBootstrap
import com.github.olehbohatyi.mep.config.KafkaConfig

// Runs against a broker with auto-create disabled, like docker-compose
object TopicBootstrapSpec extends ZIOSpecDefault:

  def spec = (suite("TopicBootstrap")(
    test("creates every topic with the configured partitions and retention") {
      for
        cfg              <- ZIO.service[KafkaConfig]
        _                <- TopicBootstrap.run
        described        <- ZIO.scoped(
                              AdminClient.make(AdminClientSettings(cfg.servers)).flatMap { admin =>
                                admin.describeTopics(cfg.topics.all) <*>
                                  admin.describeConfigs(cfg.topics.all.map(ConfigResource(ConfigResourceType.Topic, _)))
                              }
                            )
        (topics, configs) = described
        retention         = configs.map((r, c) => r.name -> c.entries("retention.ms").value)
      yield assertTrue(
        topics.keySet == cfg.topics.all.toSet,
        topics.values.forall(_.partitions.size == cfg.topicSettings.partitions),
        retention(cfg.topics.dlq) == (cfg.topicSettings.dlqRetentionHours * 3600000L).toString,
        retention(cfg.topics.raw) == (cfg.topicSettings.retentionHours * 3600000L).toString,
      )
    },
    test("is safe to run again and concurrently (gate and pipe start together)") {
      for result <- ZIO.collectAllPar(List.fill(3)(TopicBootstrap.run)).exit
      yield assertTrue(result.isSuccess)
    },
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock).provideShared(Containers.kafka)
