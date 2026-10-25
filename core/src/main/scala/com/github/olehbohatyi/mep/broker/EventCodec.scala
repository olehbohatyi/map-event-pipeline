package com.github.olehbohatyi.mep.broker

import zio.json.*

import com.github.olehbohatyi.mep.domain.MapEvent

// Kafka payloads are MapEvent JSON strings
object EventCodec:
  def encode(event: MapEvent): String                   = event.toJson
  def decode(payload: String): Either[String, MapEvent] = payload.fromJson[MapEvent]
