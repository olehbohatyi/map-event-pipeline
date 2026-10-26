package com.github.olehbohatyi.mep.it

import zio.*
import zio.kafka.admin.{ AdminClient, AdminClientSettings }
import zio.kafka.admin.AdminClient.{ ConfigResource, ConfigResourceType }
import zio.test.*

import com.github.olehbohatyi.mep.broker.TopicBootstrap
import com.github.olehbohatyi.mep.config.{ KafkaConfig, TopicsConfig }

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
    test("fails fast, without retrying, when the broker rejects the topic settings") {
      // Replication factor 3 can never succeed on this single-broker cluster
      val impossible = (cfg: KafkaConfig) =>
        cfg.copy(
          topics = TopicsConfig("rf-raw", "rf-enriched", "rf-hazards", "rf-signs", "rf-traffic", "rf-dlq"),
          topicSettings = cfg.topicSettings.copy(replicationFactor = 3),
        )
      for
        cfg    <- ZIO.service[KafkaConfig]
        // The full retry schedule takes minutes; a prompt failure proves it was not used
        result <- TopicBootstrap.run.provide(ZLayer.succeed(impossible(cfg))).exit.timeout(30.seconds)
      yield assertTrue(
        result.exists(_.isFailure),
        result.flatMap(_.causeOption).flatMap(_.failureOption).exists(TopicBootstrap.isPermanent),
      )
    },
    test("is safe to run again and concurrently (gate and pipe start together)") {
      for result <- ZIO.collectAllPar(List.fill(3)(TopicBootstrap.run)).exit
      yield assertTrue(result.isSuccess)
    },
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock).provideShared(Containers.kafka)
