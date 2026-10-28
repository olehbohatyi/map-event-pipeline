package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.telemetry.opentelemetry.common.{ Attribute, Attributes }
import zio.telemetry.opentelemetry.tracing.Tracing

import com.github.olehbohatyi.mep.broker.{ EventCodec, ProducerService, TraceHeaders }
import com.github.olehbohatyi.mep.config.KafkaConfig

// A record as read from Kafka, before decoding
final case class RawRecord(
    topic: String,
    partition: Int,
    offset: Long,
    key: Option[String],
    value: String,
    headers: Map[String, String] = Map.empty,
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

  val live: URLayer[KafkaConfig & ProducerService & EventHandler & Tracing, RecordHandler] = withRetry(defaultRetry)

  def withRetry(
      retry: Schedule[Any, Any, Any]
    ): URLayer[KafkaConfig & ProducerService & EventHandler & Tracing, RecordHandler] =
    ZLayer.fromFunction(RecordHandlerLive(_, _, _, _, retry))

  final private class RecordHandlerLive(
      kafka: KafkaConfig,
      producer: ProducerService,
      handler: EventHandler,
      tracing: Tracing,
      retry: Schedule[Any, Any, Any],
    ) extends RecordHandler:

    // Continues the trace the record's headers carry (the gate's request), across retries and the dead-letter path
    def handle(record: RawRecord): Task[Unit] =
      TraceHeaders.continue(
        tracing,
        record.headers,
        s"${record.topic} process",
        Attributes(
          Attribute.string("messaging.system", "kafka"),
          Attribute.string("messaging.destination.name", record.topic),
          Attribute.long("messaging.kafka.offset", record.offset),
        ),
      )(process(record))

    private def process(record: RawRecord): Task[Unit] =
      EventCodec.decode(record.value) match
        // A malformed record would never succeed; park it on the DLQ instead of blocking the partition
        case Left(err)    => deadLetter(record, err)
        case Right(event) =>
          tracing.setAttribute("mep.event.id", event.id.toString) *>
            handler
              .handle(event)
              .tapError(err => ZIO.logWarning(s"Event ${event.id} failed, retrying: ${err.getMessage}"))
              .retry(retry)
              .timed
              .flatMap {
                case (took, Outcome.Processed(topic)) =>
                  PipeMetrics.handled(topic, took) *> ZIO.logInfo(s"Processed event ${event.id} → $topic")
                case (took, Outcome.Duplicate)        =>
                  PipeMetrics.handled("duplicate", took) *> ZIO.logInfo(s"Skipping duplicate event ${event.id}")
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
        _   <- PipeMetrics.deadLetter
        _   <-
          ZIO.logWarning(
            s"Malformed record ${record.topic}/${record.partition}@${record.offset} sent to ${kafka.topics.dlq}: $error"
          )
      yield ()
