package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.kafka.consumer.{ Consumer, ConsumerSettings, Subscription }
import zio.kafka.serde.Serde

import com.github.olehbohatyi.mep.config.KafkaConfig

trait StreamProcessor:
  def run: Task[Unit]

object StreamProcessor:

  // A new consumer group starts from the beginning of the raw topic, not just new records
  val consumerSettings: URLayer[KafkaConfig, ConsumerSettings] =
    ZLayer.fromFunction((cfg: KafkaConfig) =>
      ConsumerSettings(cfg.servers)
        .withGroupId(cfg.consumerGroup)
        .withOffsetRetrieval(Consumer.OffsetRetrieval.Auto(Consumer.AutoOffsetStrategy.Earliest))
    )

  val live: URLayer[Consumer & KafkaConfig & RecordHandler, StreamProcessor] =
    ZLayer.fromFunction(StreamProcessorLive(_, _, _))

  final private class StreamProcessorLive(
      consumer: Consumer,
      kafka: KafkaConfig,
      handler: RecordHandler,
    ) extends StreamProcessor:

    // Offsets are committed in batches, and only for records the handler has finished with
    def run: Task[Unit] =
      consumer
        .plainStream(Subscription.topics(kafka.topics.raw), Serde.string, Serde.string)
        .mapZIO { record =>
          val raw =
            RawRecord(record.record.topic, record.partition, record.offset.offset, Option(record.key), record.value)
          handler.handle(raw).as(record.offset)
        }
        .aggregateAsync(Consumer.offsetBatches)
        .mapZIO(_.commit)
        .runDrain
