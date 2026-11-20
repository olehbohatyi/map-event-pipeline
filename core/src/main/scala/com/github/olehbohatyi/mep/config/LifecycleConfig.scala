package com.github.olehbohatyi.mep.config

import zio.*

import com.github.olehbohatyi.mep.domain.EventType

/** How long an event stays open before the pipe expires it, by type, counted from when the gate received it
  * (`createdAt`, not the client's `occurredAt`), and how often the pipe looks for events to expire.
  */
final case class LifecycleConfig(
    hazardTtl: Duration,
    trafficTtl: Duration,
    signTtl: Duration,
    sweepInterval: Duration,
  ):
  def ttl(eventType: EventType): Duration =
    eventType match
      case EventType.Hazard  => hazardTtl
      case EventType.Traffic => trafficTtl
      case EventType.Sign    => signTtl
