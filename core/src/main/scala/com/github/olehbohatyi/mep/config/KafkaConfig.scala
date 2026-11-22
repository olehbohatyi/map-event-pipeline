package com.github.olehbohatyi.mep.config

final case class TopicsConfig(
    raw: String,
    enriched: String,
    hazards: String,
    signs: String,
    traffic: String,
    dlq: String,
    // Close requests from the gate to the pipe
    status: String,
    // Events as the pipe closed them, for the live map
    closed: String,
  ):
  def all: List[String] = List(raw, enriched, hazards, signs, traffic, dlq, status, closed)

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

/** `deliveryTimeout` bounds a publish: how long the producer waits for the broker before the send fails. Kafka's
  * default (2 minutes, after up to 1 minute blocked on metadata) outlasted any client waiting for the gate.
  *
  * `linger` is how long the producer holds a record back to batch it with others; Kafka 4's default, 5 ms, is kept.
  * Every publish here is awaited, so a lone record waits out the linger: with 0, one publish to an idle compose broker
  * took about 4 ms instead of 8–12. But under load every record became a request of its own, and in CI the stress
  * profile's POST p95 rose from 18–26 ms to 390 ms (ingest to stored from 35–117 ms to 1.5 s).
  */
final case class KafkaConfig(
    bootstrapServers: String,
    consumerGroup: String,
    topics: TopicsConfig,
    topicSettings: TopicSettings,
    deliveryTimeout: zio.Duration = zio.Duration.fromSeconds(10),
    linger: zio.Duration = zio.Duration.fromMillis(5),
  ):
  def servers: List[String] = bootstrapServers.split(',').map(_.trim).filter(_.nonEmpty).toList
