package com.github.olehbohatyi.mep.pipe

import zio.*

import com.github.olehbohatyi.mep.broker.{ EventCodec, ProducerService }
import com.github.olehbohatyi.mep.config.KafkaConfig

// A record as read from Kafka, before decoding
final case class RawRecord(
    topic: String,
    partition: Int,
    offset: Long,
    key: Option[String],
    value: String,
  )

// Handles one raw record. Succeeds only when the record is fully dealt with — processed,
// skipped as a duplicate, or parked on the dead-letter topic — so its offset may be committed.
trait RecordHandler:
  def handle(record: RawRecord): Task[Unit]

object RecordHandler:

  object Headers:
    val Error           = "mep.error"
    val SourceTopic     = "mep.source.topic"
    val SourcePartition = "mep.source.partition"
    val SourceOffset    = "mep.source.offset"
    val FailedAt        = "mep.failed-at"

  // Transient failures (DB/Redis/Kafka hiccups) are retried; if they persist the stream
  // fails without committing, so the record is redelivered after restart
  val defaultRetry: Schedule[Any, Any, Any] = Schedule.exponential(200.millis) && Schedule.recurs(5)

  val live: URLayer[KafkaConfig & ProducerService & EventHandler, RecordHandler] = withRetry(defaultRetry)

  def withRetry(retry: Schedule[Any, Any, Any]): URLayer[KafkaConfig & ProducerService & EventHandler, RecordHandler] =
    ZLayer.fromFunction(RecordHandlerLive(_, _, _, retry))

  final private class RecordHandlerLive(
      kafka: KafkaConfig,
      producer: ProducerService,
      handler: EventHandler,
      retry: Schedule[Any, Any, Any],
    ) extends RecordHandler:

    def handle(record: RawRecord): Task[Unit] =
      EventCodec.decode(record.value) match
        // A malformed record would never succeed; park it on the DLQ instead of blocking the partition
        case Left(err)    => deadLetter(record, err)
        case Right(event) =>
          handler
            .handle(event)
            .tapError(err => ZIO.logWarning(s"Event ${event.id} failed, retrying: ${err.getMessage}"))
            .retry(retry)
            .flatMap {
              case Outcome.Processed(topic) => ZIO.logInfo(s"Processed event ${event.id} → $topic")
              case Outcome.Duplicate        => ZIO.logInfo(s"Skipping duplicate event ${event.id}")
            }

    private def deadLetter(record: RawRecord, error: String): Task[Unit] =
      for
        now <- Clock.instant
        _   <- producer
                 .publishRaw(
                   kafka.topics.dlq,
                   record.key,
                   record.value,
                   Map(
                     Headers.Error           -> error,
                     Headers.SourceTopic     -> record.topic,
                     Headers.SourcePartition -> record.partition.toString,
                     Headers.SourceOffset    -> record.offset.toString,
                     Headers.FailedAt        -> now.toString,
                   ),
                 )
                 .retry(retry)
        _   <-
          ZIO.logWarning(
            s"Malformed record ${record.topic}/${record.partition}@${record.offset} sent to ${kafka.topics.dlq}: $error"
          )
      yield ()
