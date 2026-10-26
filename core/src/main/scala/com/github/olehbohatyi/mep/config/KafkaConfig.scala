package com.github.olehbohatyi.mep.config

final case class TopicsConfig(
    raw: String,
    enriched: String,
    hazards: String,
    signs: String,
    traffic: String,
    dlq: String,
  ):
  def all: List[String] = List(raw, enriched, hazards, signs, traffic, dlq)

// Applied when TopicBootstrap creates missing topics; existing topics are left as they are,
// so changing these later does not alter a topic that already exists
final case class TopicSettings(
    partitions: Int,
    replicationFactor: Int,
    retentionHours: Int,
    dlqRetentionHours: Int,
  ):
  // Values no broker can accept; reported at startup rather than after the retries run out
  def problems: List[String] =
    List(
      Option.when(partitions < 1)(s"partitions must be at least 1 (got $partitions)"),
      Option.when(replicationFactor < 1 || replicationFactor > Short.MaxValue)(
        s"replication-factor must be between 1 and ${Short.MaxValue} (got $replicationFactor)"
      ),
      Option.when(retentionHours < 1)(s"retention-hours must be at least 1 (got $retentionHours)"),
      Option.when(dlqRetentionHours < 1)(s"dlq-retention-hours must be at least 1 (got $dlqRetentionHours)"),
    ).flatten

final case class KafkaConfig(
    bootstrapServers: String,
    consumerGroup: String,
    topics: TopicsConfig,
    topicSettings: TopicSettings,
  ):
  def servers: List[String] = bootstrapServers.split(',').map(_.trim).filter(_.nonEmpty).toList
