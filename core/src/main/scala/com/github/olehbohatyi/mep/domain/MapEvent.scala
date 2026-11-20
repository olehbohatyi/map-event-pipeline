package com.github.olehbohatyi.mep.domain

import zio.json.JsonCodec
import zio.schema.{ derived, Schema }

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
    // When the event was resolved or expired; absent while it is open
    closedAt: Option[Instant] = None,
  ) derives JsonCodec,
      Schema
