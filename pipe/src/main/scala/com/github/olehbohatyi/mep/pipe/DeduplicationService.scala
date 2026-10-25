package com.github.olehbohatyi.mep.pipe

import zio.*

import com.github.olehbohatyi.mep.caches.IdempotencyCache
import com.github.olehbohatyi.mep.domain.MapEvent

trait DeduplicationService:
  def isProcessed(event: MapEvent): Task[Boolean]
  def markProcessed(event: MapEvent): Task[Unit]

object DeduplicationService:

  // Own keyspace — must not overlap with the gate's idempotency keys
  val Namespace = "mep:pipe:processed"

  private val ProcessedTtl = 48.hours

  val live: URLayer[IdempotencyCache, DeduplicationService] =
    ZLayer.fromFunction(DeduplicationServiceLive(_))

  final private class DeduplicationServiceLive(cache: IdempotencyCache) extends DeduplicationService:

    def isProcessed(event: MapEvent): Task[Boolean] =
      cache.isMarked(event.id.toString)

    def markProcessed(event: MapEvent): Task[Unit] =
      cache.mark(event.id.toString, ProcessedTtl)
