package com.github.olehbohatyi.mep.broker

import zio.test.*

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
  )
