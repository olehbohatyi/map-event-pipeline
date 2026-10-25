package com.github.olehbohatyi.mep.broker

import zio.*
import zio.kafka.producer.{ Producer, ProducerSettings }
import zio.kafka.serde.Serde

import com.github.olehbohatyi.mep.config.KafkaConfig
import com.github.olehbohatyi.mep.domain.MapEvent

trait ProducerService:
  def publish(topic: String, event: MapEvent): Task[Unit]

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
