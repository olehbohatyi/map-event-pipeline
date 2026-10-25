package com.github.olehbohatyi.mep.domain

import zio.json.JsonCodec

import java.time.Instant
import java.util.UUID

final case class MapEvent(
    id: UUID,
    eventType: EventType,
    severity: Severity,
    status: EventStatus,
    location: GeoPoint,
    description: Option[String],
    region: Option[String],
    occurredAt: Instant,
    createdAt: Instant,
  ) derives JsonCodec
