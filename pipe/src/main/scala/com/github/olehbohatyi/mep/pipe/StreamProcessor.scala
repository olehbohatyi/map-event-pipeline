package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.kafka.consumer.{ Consumer, ConsumerSettings, Subscription }
import zio.kafka.serde.Serde

import com.github.olehbohatyi.mep.broker.EventCodec
import com.github.olehbohatyi.mep.config.KafkaConfig

trait StreamProcessor:
  def run: Task[Unit]

object StreamProcessor:

  // Transient failures (DB/Redis/Kafka hiccups) are retried; if they persist the stream
  // fails without committing, so the record is redelivered after restart
  private val retryPolicy = Schedule.exponential(200.millis) && Schedule.recurs(5)

  // A new consumer group starts from the beginning of the raw topic, not just new records
  val consumerSettings: URLayer[KafkaConfig, ConsumerSettings] =
    ZLayer.fromFunction((cfg: KafkaConfig) =>
      ConsumerSettings(cfg.servers)
        .withGroupId(cfg.consumerGroup)
        .withOffsetRetrieval(Consumer.OffsetRetrieval.Auto(Consumer.AutoOffsetStrategy.Earliest))
    )

  val live: URLayer[Consumer & KafkaConfig & EventHandler, StreamProcessor] =
    ZLayer.fromFunction(StreamProcessorLive(_, _, _))

  final private class StreamProcessorLive(
      consumer: Consumer,
      kafka: KafkaConfig,
      handler: EventHandler,
    ) extends StreamProcessor:

    def run: Task[Unit] =
      consumer
        .plainStream(Subscription.topics(kafka.topics.raw), Serde.string, Serde.string)
        .mapZIO(record => process(record.key, record.value).as(record.offset))
        .aggregateAsync(Consumer.offsetBatches)
        .mapZIO(_.commit)
        .runDrain

    private def process(key: String, payload: String): Task[Unit] =
      EventCodec.decode(payload) match
        // A malformed record would never succeed; skip it instead of blocking the partition
        case Left(err)    => ZIO.logWarning(s"Skipping malformed event (key=$key): $err")
        case Right(event) =>
          handler
            .handle(event)
            .tapError(err => ZIO.logWarning(s"Event ${event.id} failed, retrying: ${err.getMessage}"))
            .retry(retryPolicy)
            .flatMap {
              case Outcome.Processed(topic) => ZIO.logInfo(s"Processed event ${event.id} → $topic")
              case Outcome.Duplicate        => ZIO.logInfo(s"Skipping duplicate event ${event.id}")
            }
