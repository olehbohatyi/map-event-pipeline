package com.github.olehbohatyi.mep.config

final case class TopicsConfig(
    raw: String,
    enriched: String,
    hazards: String,
    signs: String,
    traffic: String,
  )

final case class KafkaConfig(
    bootstrapServers: String,
    consumerGroup: String,
    topics: TopicsConfig,
  ):
  def servers: List[String] = bootstrapServers.split(',').map(_.trim).filter(_.nonEmpty).toList
