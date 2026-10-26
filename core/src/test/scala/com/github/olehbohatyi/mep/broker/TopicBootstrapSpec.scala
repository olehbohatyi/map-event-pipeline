package com.github.olehbohatyi.mep.broker

import zio.*
import zio.test.*

import java.util.concurrent.ExecutionException

import org.apache.kafka.common.errors.{ InvalidReplicationFactorException, TimeoutException }
import com.github.olehbohatyi.mep.testkit.TestKafka

object TopicBootstrapSpec extends ZIOSpecDefault:

  private val hour = 3600L * 1000

  def spec = suite("TopicBootstrap.newTopics")(
    test("declares every pipeline topic with the configured partitions and replication") {
      val topics = TopicBootstrap.newTopics(TestKafka.config())
      assertTrue(
        topics.map(_.name).toSet == Set("raw", "enriched", "hazards", "signs", "traffic", "dlq"),
        topics.forall(t => t.numPartitions == 2 && t.replicationFactor == 1.toShort),
      )
    },
    test("keeps dead letters longer than regular events") {
      val retention = TopicBootstrap.newTopics(TestKafka.config()).map(t => t.name -> t.configs("retention.ms")).toMap
      assertTrue(retention("dlq") == (2 * hour).toString, retention("raw") == hour.toString)
    },
    test("accepts the default settings and lists every bad value otherwise") {
      val bad =
        TestKafka.settings.copy(partitions = 0, replicationFactor = 0, retentionHours = -1, dlqRetentionHours = 0)
      assertTrue(
        TestKafka.settings.problems.isEmpty,
        bad.problems.size == 4,
        bad.problems.exists(_.contains("partitions")),
        bad.problems.exists(_.contains("replication-factor")),
      )
    },
    test("rejects invalid settings immediately instead of retrying against the broker") {
      val cfg = TestKafka.config().copy(topicSettings = TestKafka.settings.copy(partitions = 0))
      for exit <- TopicBootstrap.run.provide(ZLayer.succeed(cfg)).exit
      yield assertTrue(
        exit.isFailure,
        exit
          .causeOption
          .flatMap(_.failureOption)
          .exists(e => e.isInstanceOf[IllegalArgumentException] && e.getMessage.contains("partitions")),
      )
    },
    test("treats broker rejections as permanent, even when wrapped, but connection trouble as transient") {
      assertTrue(
        TopicBootstrap.isPermanent(InvalidReplicationFactorException("only 1 broker")),
        TopicBootstrap.isPermanent(ExecutionException(InvalidReplicationFactorException("only 1 broker"))),
        !TopicBootstrap.isPermanent(TimeoutException("broker not reachable")),
        !TopicBootstrap.isPermanent(RuntimeException("boom")),
      )
    },
  )
