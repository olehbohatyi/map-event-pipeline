package com.github.olehbohatyi.mep.gate

import zio.json.*

import java.time.Instant
import java.util.UUID

import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.stores.MapEventStat

// ── HTTP request/response DTOs ───────────────────────────────────────────────

final case class EventRequest(
    eventType: EventType,
    severity: Severity,
    location: GeoPoint,
    description: Option[String],
    occurredAt: Option[Instant],
  ) derives JsonDecoder

final case class IngestResponse(id: UUID, status: String) derives JsonEncoder

final case class NearbyResponse(region: String, count: Int, events: List[MapEvent]) derives JsonEncoder

final case class StatsResponse(region: Option[String], stats: List[MapEventStat]) derives JsonEncoder

final case class ErrorResponse(error: String) derives JsonEncoder
