package com.github.olehbohatyi.mep.stat

import scala.concurrent.duration.*

// The streaming job runs under spark-submit like StatApp, so it reads plain env vars too
final case class TrendsConfig(
    kafkaServers: String,
    topic: String,
    checkpointDir: String,
    trigger: FiniteDuration,
    db: StatConfig,
  )

object TrendsConfig:
  def fromEnv(env: Map[String, String] = sys.env): TrendsConfig =
    TrendsConfig(
      kafkaServers = env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
      topic = env.getOrElse("TRENDS_TOPIC", "map.events.enriched"),
      // Must survive restarts: it holds the Kafka offsets and the de-duplication state
      checkpointDir = env.getOrElse("TRENDS_CHECKPOINT_DIR", "/tmp/mep/trends-checkpoint"),
      trigger = env.get("TRENDS_TRIGGER_SECONDS").flatMap(_.toIntOption).getOrElse(30).seconds,
      db = StatConfig.fromEnv(env),
    )
