package com.github.olehbohatyi.mep.gate

import zio.*

import java.util.UUID

import com.github.olehbohatyi.mep.broker.ProducerService
import com.github.olehbohatyi.mep.caches.IdempotencyCache
import com.github.olehbohatyi.mep.config.KafkaConfig
import com.github.olehbohatyi.mep.domain.*

enum IngestionResult:
  case Accepted(id: UUID)
  case Duplicate(id: UUID)

trait IngestionService:
  def ingest(request: EventRequest): Task[IngestionResult]

object IngestionService:

  val Namespace = "mep:idempotency"

  private val IdempotencyTtl = 24.hours

  val live: URLayer[ProducerService & IdempotencyCache & KafkaConfig, IngestionService] =
    ZLayer.fromFunction(IngestionServiceLive(_, _, _))

  final private class IngestionServiceLive(
      producer: ProducerService,
      cache: IdempotencyCache,
      kafka: KafkaConfig,
    ) extends IngestionService:

    def ingest(request: EventRequest): Task[IngestionResult] =
      Random.nextUUID.flatMap { id =>
        cache.isMarked(id.toString).flatMap {
          case true  => ZIO.succeed(IngestionResult.Duplicate(id))
          case false => publish(id, request) <* cache.mark(id.toString, IdempotencyTtl)
        }
      }

    private def publish(id: UUID, request: EventRequest): Task[IngestionResult] =
      for
        now  <- Clock.instant
        event = MapEvent(
                  id = id,
                  eventType = request.eventType,
                  severity = request.severity,
                  status = EventStatus.Raw,
                  location = request.location,
                  description = request.description,
                  region = None,
                  occurredAt = request.occurredAt.getOrElse(now),
                  createdAt = now,
                )
        _    <- producer.publish(kafka.topics.raw, event)
      yield IngestionResult.Accepted(id)
