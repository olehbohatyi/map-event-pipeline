package com.github.olehbohatyi.mep.broker

import zio.json.*

import com.github.olehbohatyi.mep.domain.{ MapEvent, StatusChange }

// Kafka payloads are MapEvent JSON strings; the status topic carries StatusChange JSON
object EventCodec:
  def encode(event: MapEvent): String                   = event.toJson
  def decode(payload: String): Either[String, MapEvent] = payload.fromJson[MapEvent]

  def encodeChange(change: StatusChange): String                  = change.toJson
  def decodeChange(payload: String): Either[String, StatusChange] = payload.fromJson[StatusChange]
