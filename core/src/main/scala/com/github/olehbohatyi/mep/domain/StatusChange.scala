package com.github.olehbohatyi.mep.domain

import zio.json.JsonCodec

import java.time.Instant
import java.util.UUID

// A request to close a stored event, sent by the gate to the pipe on the status topic (keyed by event id)
final case class StatusChange(id: UUID, status: EventStatus, requestedAt: Instant) derives JsonCodec
