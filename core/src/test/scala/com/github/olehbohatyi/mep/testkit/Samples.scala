package com.github.olehbohatyi.mep.testkit

import java.time.Instant
import java.util.UUID

import com.github.olehbohatyi.mep.domain.*

object Samples:
  val Kyiv: GeoPoint = GeoPoint(50.4501, 30.5234)
  val Lviv: GeoPoint = GeoPoint(49.8397, 24.0297)

  def event(
      location: GeoPoint = Kyiv,
      eventType: EventType = EventType.Hazard,
      severity: Severity = Severity.Low,
      status: EventStatus = EventStatus.Raw,
      region: Option[String] = None,
    ): MapEvent =
    val now = Instant.parse("2026-01-01T10:00:00Z")
    MapEvent(UUID.randomUUID(), eventType, severity, status, location, Some("pothole"), region, now, now)
