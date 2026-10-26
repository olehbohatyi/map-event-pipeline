package com.github.olehbohatyi.mep.testkit

import com.github.olehbohatyi.mep.config.{ KafkaConfig, TopicsConfig, TopicSettings }

// Kafka config for tests; topic names are short so assertions stay readable
object TestKafka:
  val topics: TopicsConfig    = TopicsConfig("raw", "enriched", "hazards", "signs", "traffic", "dlq")
  val settings: TopicSettings =
    TopicSettings(partitions = 2, replicationFactor = 1, retentionHours = 1, dlqRetentionHours = 2)

  def config(servers: String = "unused:9092", group: String = "g"): KafkaConfig =
    KafkaConfig(servers, group, topics, settings)
