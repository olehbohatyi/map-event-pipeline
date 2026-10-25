package com.github.olehbohatyi.mep.stores

import zio.*

import java.time.Instant
import java.util.UUID
import javax.sql.DataSource

import io.getquill.*
import io.scalaland.chimney.dsl.*
import com.github.olehbohatyi.mep.domain.*

// Row representation of the map_events table
final case class MapEventRow(
    id: UUID,
    eventType: EventType,
    severity: Severity,
    status: EventStatus,
    lat: Double,
    lon: Double,
    description: Option[String],
    region: Option[String],
    occurredAt: Instant,
    createdAt: Instant,
  )

object MapEventRow:

  def from(event: MapEvent): MapEventRow =
    event
      .into[MapEventRow]
      .withFieldComputed(_.lat, _.location.lat)
      .withFieldComputed(_.lon, _.location.lon)
      .transform

  def toDomain(row: MapEventRow): MapEvent =
    row
      .into[MapEvent]
      .withFieldComputed(_.location, r => GeoPoint(r.lat, r.lon))
      .transform

trait EventRepo:
  def insert(event: MapEvent): Task[Unit]
  def findById(id: UUID): Task[Option[MapEvent]]
  def findNearby(center: GeoPoint, radiusKm: Double, limit: Int): Task[List[MapEvent]]

object EventRepo:

  val live: URLayer[DataSource, EventRepo] =
    ZLayer.fromFunction(EventRepoLive(_))

  final private class EventRepoLive(ds: DataSource) extends EventRepo:

    import QuillContext.{ *, given }

    inline private def events = quote(querySchema[MapEventRow]("map_events"))

    private val env = ZEnvironment(ds)

    def insert(event: MapEvent): Task[Unit] =
      val row = MapEventRow.from(event)
      run(events.insertValue(lift(row)))
        .unit
        .provideEnvironment(env)

    def findById(id: UUID): Task[Option[MapEvent]] =
      run(events.filter(_.id == lift(id)))
        .map(_.headOption.map(MapEventRow.toDomain))
        .provideEnvironment(env)

    // Square box of ±radius around the center (1° ≈ 111 km)
    def findNearby(center: GeoPoint, radiusKm: Double, limit: Int): Task[List[MapEvent]] =
      val delta = radiusKm / 111.0
      run(
        events
          .filter(r =>
            r.lat >= lift(center.lat - delta) && r.lat <= lift(center.lat + delta) &&
            r.lon >= lift(center.lon - delta) && r.lon <= lift(center.lon + delta)
          )
          .take(lift(limit))
      )
        .map(_.map(MapEventRow.toDomain))
        .provideEnvironment(env)
