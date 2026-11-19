package com.github.olehbohatyi.mep.broker

import zio.*
import zio.kafka.producer.{ Producer, ProducerSettings }
import zio.kafka.serde.Serde
import zio.telemetry.opentelemetry.common.{ Attribute, Attributes }
import zio.telemetry.opentelemetry.tracing.Tracing

import java.nio.charset.StandardCharsets.UTF_8

import io.opentelemetry.api.trace.SpanKind
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import scala.jdk.CollectionConverters.*
import com.github.olehbohatyi.mep.config.KafkaConfig
import com.github.olehbohatyi.mep.domain.MapEvent

trait ProducerService:
  def publish(topic: String, event: MapEvent): Task[Unit]
  // Publishes a payload as-is, e.g. an unparseable record to the dead-letter topic
  def publishRaw(topic: String, key: Option[String], value: String, headers: Map[String, String]): Task[Unit]

object ProducerService:

  val live: ZLayer[KafkaConfig & Tracing, Throwable, ProducerService] =
    ZLayer.scoped {
      for
        cfg      <- ZIO.service[KafkaConfig]
        tracing  <- ZIO.service[Tracing]
        producer <- Producer.make(settings(cfg))
      yield ProducerServiceLive(producer, tracing)
    }

  // A send fails after deliveryTimeout, blocked on metadata or waiting for acks; Kafka requires the request timeout to
  // fit in it (delivery.timeout.ms >= linger.ms + request.timeout.ms)
  private[broker] def settings(cfg: KafkaConfig): ProducerSettings =
    val delivery = cfg.deliveryTimeout.toMillis
    ProducerSettings(cfg.servers)
      .withProperty("delivery.timeout.ms", delivery.toString)
      .withProperty("request.timeout.ms", (delivery / 2).toString)
      .withProperty("max.block.ms", delivery.toString)

  final private class ProducerServiceLive(producer: Producer, tracing: Tracing) extends ProducerService:

    // Keyed by event id so all versions of an event land on the same partition
    def publish(topic: String, event: MapEvent): Task[Unit] =
      send(topic, Some(event.id.toString), EventCodec.encode(event), Map.empty)

    def publishRaw(topic: String, key: Option[String], value: String, headers: Map[String, String]): Task[Unit] =
      send(topic, key, value, headers)

    // Every record carries the trace context of its producer span, so the consumer continues the same trace
    private def send(topic: String, key: Option[String], value: String, headers: Map[String, String]): Task[Unit] =
      tracing.span(
        s"$topic publish",
        SpanKind.PRODUCER,
        Attributes(Attribute.string("messaging.system", "kafka"), Attribute.string("messaging.destination.name", topic)),
      ) {
        TraceHeaders.ofCurrentSpan(tracing).flatMap { trace =>
          val kafkaHeaders: List[Header] =
            (headers ++ trace).map((k, v) => RecordHeader(k, v.getBytes(UTF_8))).toList
          val noPartition: Integer       = null // let the partitioner choose
          val record                     =
            ProducerRecord[String, String](topic, noPartition, key.orNull, value, kafkaHeaders.asJava)
          producer.produce(record, Serde.string, Serde.string).unit
        }
      }
