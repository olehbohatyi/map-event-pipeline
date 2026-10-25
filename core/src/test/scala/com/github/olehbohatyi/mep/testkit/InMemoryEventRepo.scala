package com.github.olehbohatyi.mep.testkit

import zio.*

import java.util.UUID

import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.stores.EventRepo

final class InMemoryEventRepo(rows: Ref[Map[UUID, MapEvent]]) extends EventRepo:
  def insert(event: MapEvent): Task[Unit]                                              = rows.update(_ + (event.id -> event))
  def findById(id: UUID): Task[Option[MapEvent]]                                       = rows.get.map(_.get(id))
  def findNearby(center: GeoPoint, radiusKm: Double, limit: Int): Task[List[MapEvent]] =
    rows
      .get
      .map(
        _.values
          .toList
          .map(e => e -> GeoPoint.distanceKm(center, e.location))
          .filter(_._2 <= radiusKm)
          .sortBy(_._2)
          .take(limit)
          .map(_._1)
      )
  def all: UIO[List[MapEvent]]                                                         = rows.get.map(_.values.toList)

object InMemoryEventRepo:
  val layer: ULayer[InMemoryEventRepo] = ZLayer(Ref.make(Map.empty[UUID, MapEvent]).map(InMemoryEventRepo(_)))
