package com.github.olehbohatyi.mep.stores

import zio.*

import java.time.Instant
import java.util.UUID
import javax.sql.DataSource

import com.github.olehbohatyi.mep.domain.*
import io.getquill.*
import io.scalaland.chimney.dsl.*

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
  // Returns false when an event with the same id is already stored
  def insert(event: MapEvent): Task[Boolean]
  def findById(id: UUID): Task[Option[MapEvent]]
  // Events within radiusKm of center, nearest first
  def findNearby(center: GeoPoint, radiusKm: Double, limit: Int): Task[List[MapEvent]]
  // The latest events by occurrence time, newest first
  def findRecent(limit: Int): Task[List[MapEvent]]

object EventRepo:

  val live: URLayer[DataSource, EventRepo] =
    ZLayer.fromFunction(EventRepoLive(_))

  final private class EventRepoLive(ds: DataSource) extends EventRepo:

    import QuillContext.{ *, given }

    inline private def events = quote(querySchema[MapEventRow]("map_events"))

    private val env = ZEnvironment(ds)

    def insert(event: MapEvent): Task[Boolean] =
      val row = MapEventRow.from(event)
      run(events.insertValue(lift(row)).onConflictIgnore)
        .map(_ > 0)
        .provideEnvironment(env)

    def findById(id: UUID): Task[Option[MapEvent]] =
      run(events.filter(_.id == lift(id)))
        .map(_.headOption.map(MapEventRow.toDomain))
        .provideEnvironment(env)

    // PostGIS answers from the GiST index on the geography column (generated from lat/lon, see V2): exact
    // great-circle distance in metres, nearest first, correct across the poles and the antimeridian
    def findNearby(center: GeoPoint, radiusKm: Double, limit: Int): Task[List[MapEvent]] =
      val lat    = center.lat
      val lon    = center.lon
      val meters = radiusKm * 1000
      run(
        events
          .filter(_ =>
            sql"ST_DWithin(geog, ST_SetSRID(ST_MakePoint(${lift(lon)}, ${lift(lat)}), 4326)::geography, ${lift(meters)})".asCondition
          )
          .sortBy(_ => sql"geog <-> ST_SetSRID(ST_MakePoint(${lift(lon)}, ${lift(lat)}), 4326)::geography".as[Double])
          .take(lift(limit))
      )
        .map(_.map(MapEventRow.toDomain))
        .provideEnvironment(env)

    // Served by idx_map_events_occurred (occurred_at DESC, V1)
    def findRecent(limit: Int): Task[List[MapEvent]] =
      run(events.sortBy(_.occurredAt)(using Ord.desc).take(lift(limit)))
        .map(_.map(MapEventRow.toDomain))
        .provideEnvironment(env)
