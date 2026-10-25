package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.redis.*

import com.github.olehbohatyi.mep.config.KafkaConfig
import com.github.olehbohatyi.mep.domain.{ EventType, MapEvent, Severity }

// Routes an event to its typed topic and raises Low severity to Medium when a
// region sees a burst of events of the same type (Redis counter per time window)
trait EscalationService:
  def topicFor(event: MapEvent): String
  def escalate(event: MapEvent): Task[MapEvent]

object EscalationService:

  val Threshold = 5L // events per region and type within the window
  val Window    = 5.minutes

  val live: URLayer[Redis & KafkaConfig, EscalationService] =
    ZLayer.fromFunction(EscalationServiceLive(_, _))

  final private class EscalationServiceLive(redis: Redis, kafka: KafkaConfig) extends EscalationService:

    def topicFor(event: MapEvent): String =
      event.eventType match
        case EventType.Hazard  => kafka.topics.hazards
        case EventType.Sign    => kafka.topics.signs
        case EventType.Traffic => kafka.topics.traffic

    def escalate(event: MapEvent): Task[MapEvent] =
      val key = s"mep:escalation:${event.region.getOrElse("unknown")}:${event.eventType}"
      for
        count <- redis.incr(key)
        _     <- redis.expire(key, Window).when(count == 1L)
      yield
        if count >= Threshold && event.severity == Severity.Low then event.copy(severity = Severity.Medium)
        else event
