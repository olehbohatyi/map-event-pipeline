package com.github.olehbohatyi.mep.gate

import zio.*

import java.util.UUID

import com.github.olehbohatyi.mep.broker.{ EventCodec, ProducerService }
import com.github.olehbohatyi.mep.config.KafkaConfig
import com.github.olehbohatyi.mep.domain.{ EventStatus, StatusChange }
import com.github.olehbohatyi.mep.stores.EventRepo

enum ResolveResult:
  // The request was published; the pipe closes the event
  case Requested(id: UUID)
  case NotFound(id: UUID)
  case AlreadyClosed(id: UUID, status: EventStatus)

trait ResolutionService:
  def resolve(id: UUID): Task[ResolveResult]

object ResolutionService:

  val live: URLayer[EventRepo & ProducerService & KafkaConfig, ResolutionService] =
    ZLayer.fromFunction(ResolutionServiceLive(_, _, _))

  /** Only a stored, open event can be resolved, so the pipe always finds the event it is asked to close. The gate does
    * not write Postgres: the request goes through Kafka like every other change, keyed by the event id. Resolving twice
    * is harmless: the pipe closes an event once and leaves a closed one as it is.
    */
  final private class ResolutionServiceLive(repo: EventRepo, producer: ProducerService, kafka: KafkaConfig)
      extends ResolutionService:

    def resolve(id: UUID): Task[ResolveResult] =
      repo.findById(id).flatMap {
        case None                                 => ZIO.succeed(ResolveResult.NotFound(id))
        case Some(event) if event.status.isClosed => ZIO.succeed(ResolveResult.AlreadyClosed(id, event.status))
        case Some(_)                              =>
          for
            now   <- Clock.instant
            change = StatusChange(id, EventStatus.Resolved, now)
            _     <- producer.publishRaw(kafka.topics.status, Some(id.toString), EventCodec.encodeChange(change), Map.empty)
          yield ResolveResult.Requested(id)
      }
