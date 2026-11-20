package com.github.olehbohatyi.mep.testkit

import zio.*

import java.time.Instant
import java.util.UUID

import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.stores.EventRepo

final class InMemoryEventRepo(rows: Ref[Map[UUID, MapEvent]]) extends EventRepo:
  def insert(event: MapEvent): Task[Boolean]                                               =
    rows.modify(m => if m.contains(event.id) then (false, m) else (true, m + (event.id -> event)))
  def findById(id: UUID): Task[Option[MapEvent]]                                           = rows.get.map(_.get(id))
  def findNearby(center: GeoPoint, radiusKm: Double, limit: Int): Task[List[MapEvent]]     =
    rows
      .get
      .map(
        _.values
          .toList
          .filterNot(_.status.isClosed)
          .map(e => e -> GeoPoint.distanceKm(center, e.location))
          .filter(_._2 <= radiusKm)
          .sortBy(_._2)
          .take(limit)
          .map(_._1)
      )
  def findRecent(limit: Int): Task[List[MapEvent]]                                         =
    rows.get.map(_.values.toList.filterNot(_.status.isClosed).sortBy(_.occurredAt).reverse.take(limit))
  def close(id: UUID, status: EventStatus, at: Instant): Task[Boolean]                     =
    rows.modify { m =>
      m.get(id).filterNot(_.status.isClosed) match
        case Some(e) => (true, m + (id -> e.copy(status = status, closedAt = Some(at))))
        case None    => (false, m)
    }
  def findExpired(eventType: EventType, cutoff: Instant, limit: Int): Task[List[MapEvent]] =
    rows
      .get
      .map(
        _.values
          .filter(e => e.eventType == eventType && e.createdAt.isBefore(cutoff) && !e.status.isClosed)
          .toList
          .sortBy(_.createdAt)
          .take(limit)
      )
  def all: UIO[List[MapEvent]]                                                             = rows.get.map(_.values.toList)

object InMemoryEventRepo:
  val layer: ULayer[InMemoryEventRepo] = ZLayer(Ref.make(Map.empty[UUID, MapEvent]).map(InMemoryEventRepo(_)))
