package com.github.olehbohatyi.mep.gate

import zio.json.*
import zio.schema.{ derived, Schema }

import java.time.Instant
import java.util.UUID

import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.stores.{ MapEventStat, MapEventTrend }

// ── HTTP request/response DTOs ───────────────────────────────────────────────
// zio-json writes the bytes (so `None` fields stay omitted); zio-schema only describes them for the OpenAPI spec.

final case class EventRequest(
    eventType: EventType,
    severity: Severity,
    location: GeoPoint,
    description: Option[String],
    occurredAt: Option[Instant],
  ) derives JsonCodec,
      Schema

// 202: first time this event (or Idempotency-Key) is seen
final case class IngestAccepted(id: UUID, status: String) derives JsonCodec, Schema

// 200: the Idempotency-Key was already used, nothing was published again
final case class IngestDuplicate(id: UUID, status: String) derives JsonCodec, Schema

final case class NearbyResponse(region: String, count: Int, events: List[MapEvent]) derives JsonCodec, Schema

final case class RecentResponse(count: Int, events: List[MapEvent]) derives JsonCodec, Schema

final case class StatsResponse(region: Option[String], stats: List[MapEventStat]) derives JsonCodec, Schema

// Hourly counts from the streaming job, oldest hour first; hours without events are absent
final case class HourlyResponse(region: Option[String], hours: Int, counts: List[MapEventTrend])
    derives JsonCodec,
      Schema

// Every error body is `{"error": "..."}`; the subtype only picks the status code
sealed trait ApiError:
  def error: String

object ApiError:
  final case class BadRequest(error: String) extends ApiError derives JsonCodec, Schema
  final case class NotFound(error: String)   extends ApiError derives JsonCodec, Schema
  final case class Internal(error: String)   extends ApiError derives JsonCodec, Schema
