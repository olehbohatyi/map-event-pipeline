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
  // The key's event was published before
  case Duplicate(id: UUID)
  // Another request with the same key is still publishing, or just failed to: the client should retry
  case InProgress(id: UUID)

trait IngestionService:
  // With an Idempotency-Key, retries of the same request map to the same event id and are published once
  def ingest(request: EventRequest, idempotencyKey: Option[String]): Task[IngestionResult]

object IngestionService:

  val Namespace = "mep:gate:idem"

  private val IdempotencyTtl = 24.hours

  // A key is `Pending` while its event is being published and `IdempotencyCache.Marked` once it was. A retry that
  // finds it pending is not told the event was published: the first attempt may still fail. Pending expires after a
  // publish could have taken (twice the delivery timeout), so a gate that dies mid-publish blocks the key that long,
  // not for a day.
  val Pending = "pending"

  private def pendingTtl(kafka: KafkaConfig): Duration = kafka.deliveryTimeout * 2 + 30.seconds

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
          cache.tryMark(key, pendingTtl(kafka), Pending).flatMap {
            case true  =>
              publish(id, request)
                // Release the key if publishing fails so the client can retry
                .onError(_ => cache.unmark(key).ignoreLogged)
                // If this fails the key stays pending until it expires; a retry after that publishes the same id
                // again, which the pipe drops as a duplicate
                .tap(_ => cache.mark(key, IdempotencyTtl).ignoreLogged)
            case false =>
              cache.get(key).map {
                // None: the first attempt failed and released the key in between
                case Some(Pending) | None => IngestionResult.InProgress(id)
                case Some(_)              => IngestionResult.Duplicate(id)
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
