package com.github.olehbohatyi.mep.gate

import zio.*

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import com.github.olehbohatyi.mep.broker.ProducerService
import com.github.olehbohatyi.mep.caches.IdempotencyCache
import com.github.olehbohatyi.mep.config.KafkaConfig
import com.github.olehbohatyi.mep.domain.*

enum IngestionResult:
  case Accepted(id: UUID)
  case Duplicate(id: UUID)

trait IngestionService:
  // With an Idempotency-Key, retries of the same request map to the same event id and are published once
  def ingest(request: EventRequest, idempotencyKey: Option[String]): Task[IngestionResult]

object IngestionService:

  val Namespace = "mep:gate:idem"

  private val IdempotencyTtl = 24.hours

  def eventId(idempotencyKey: String): UUID =
    UUID.nameUUIDFromBytes(s"mep-gate:$idempotencyKey".getBytes(UTF_8))

  val live: URLayer[ProducerService & IdempotencyCache & KafkaConfig, IngestionService] =
    ZLayer.fromFunction(IngestionServiceLive(_, _, _))

  final private class IngestionServiceLive(
      producer: ProducerService,
      cache: IdempotencyCache,
      kafka: KafkaConfig,
    ) extends IngestionService:

    def ingest(request: EventRequest, idempotencyKey: Option[String]): Task[IngestionResult] =
      idempotencyKey match
        case None      => Random.nextUUID.flatMap(publish(_, request))
        case Some(key) =>
          val id = eventId(key)
          cache.tryMark(key, IdempotencyTtl).flatMap {
            case false => ZIO.succeed(IngestionResult.Duplicate(id))
            // Release the key if publishing fails so the client can retry
            case true  => publish(id, request).onError(_ => cache.unmark(key).ignoreLogged)
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
