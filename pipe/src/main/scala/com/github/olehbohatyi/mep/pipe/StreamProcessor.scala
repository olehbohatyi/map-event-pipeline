package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.kafka.consumer.{ Consumer, ConsumerSettings, Subscription }
import zio.kafka.serde.Serde

import java.nio.charset.StandardCharsets.UTF_8

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

    /** Partitions are processed in parallel, each in order: an event's partition follows its id (the record key), so
      * order only matters within one. Offsets are committed in batches (the latest per partition), and only for records
      * the handler has finished with. Before this, one sequential stream over all partitions capped the pipe at about
      * 24 events/s (load test baseline).
      */
    def run: Task[Unit] =
      consumer
        .partitionedStream(Subscription.topics(kafka.topics.raw), Serde.string, Serde.string)
        .flatMapPar(Int.MaxValue) { (_, partition) =>
          partition.mapZIO { record =>
            // The last value of each header wins, as Kafka's own lastHeader does
            val headers = record.record.headers.toArray.map(h => h.key -> String(h.value, UTF_8)).toMap
            val raw     =
              RawRecord(
                record.record.topic,
                record.partition,
                record.offset.offset,
                Option(record.key),
                record.value,
                headers,
              )
            handler.handle(raw).as(record.offset)
          }
        }
        .aggregateAsync(Consumer.offsetBatches)
        .mapZIO(_.commit)
        .runDrain
