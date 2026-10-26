package com.github.olehbohatyi.mep.broker

import zio.*
import zio.kafka.producer.{ Producer, ProducerSettings }
import zio.kafka.serde.Serde

import java.nio.charset.StandardCharsets.UTF_8

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

  val live: ZLayer[KafkaConfig, Throwable, ProducerService] =
    ZLayer.scoped {
      for
        cfg      <- ZIO.service[KafkaConfig]
        producer <- Producer.make(ProducerSettings(cfg.servers))
      yield ProducerServiceLive(producer)
    }

  final private class ProducerServiceLive(producer: Producer) extends ProducerService:

    // Keyed by event id so all versions of an event land on the same partition
    def publish(topic: String, event: MapEvent): Task[Unit] =
      producer
        .produce(topic, event.id.toString, EventCodec.encode(event), Serde.string, Serde.string)
        .unit

    def publishRaw(topic: String, key: Option[String], value: String, headers: Map[String, String]): Task[Unit] =
      val kafkaHeaders: List[Header] = headers.map((k, v) => RecordHeader(k, v.getBytes(UTF_8))).toList
      val noPartition: Integer       = null // let the partitioner choose
      val record                     = ProducerRecord[String, String](topic, noPartition, key.orNull, value, kafkaHeaders.asJava)
      producer.produce(record, Serde.string, Serde.string).unit
