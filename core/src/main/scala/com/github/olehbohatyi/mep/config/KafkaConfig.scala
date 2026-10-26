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

// Applied when TopicBootstrap creates missing topics; existing topics are left as they are
final case class TopicSettings(
    partitions: Int,
    replicationFactor: Int,
    retentionHours: Int,
    dlqRetentionHours: Int,
  )

final case class KafkaConfig(
    bootstrapServers: String,
    consumerGroup: String,
    topics: TopicsConfig,
    topicSettings: TopicSettings,
  ):
  def servers: List[String] = bootstrapServers.split(',').map(_.trim).filter(_.nonEmpty).toList
