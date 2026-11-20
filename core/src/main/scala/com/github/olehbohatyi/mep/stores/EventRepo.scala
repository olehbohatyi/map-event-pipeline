package com.github.olehbohatyi.mep.stores

import zio.*

import java.time.Instant
import java.util.UUID
import javax.sql.DataSource

import com.github.olehbohatyi.mep.domain.*
import io.getquill.*
import io.getquill.extras.InstantOps
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
    closedAt: Option[Instant],
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
  // Open events within radiusKm of center, nearest first
  def findNearby(center: GeoPoint, radiusKm: Double, limit: Int): Task[List[MapEvent]]
  // The latest open events by occurrence time, newest first
  def findRecent(limit: Int): Task[List[MapEvent]]
  // Closes the event if it is still open; false when it is closed already or not stored
  def close(id: UUID, status: EventStatus, at: Instant): Task[Boolean]
  // Up to `limit` open events of this type received before `cutoff`, oldest first
  def findExpired(eventType: EventType, cutoff: Instant, limit: Int): Task[List[MapEvent]]

object EventRepo:

  val live: URLayer[DataSource, EventRepo] =
    ZLayer.fromFunction(EventRepoLive(_))

  final private class EventRepoLive(ds: DataSource) extends EventRepo:

    import QuillContext.{ *, given }

    inline private def events = quote(querySchema[MapEventRow]("map_events"))

    // Raw and Enriched: the statuses `EventStatus.isClosed` is false for
    inline private def open =
      quote((e: MapEventRow) => e.status == lift(EventStatus.Raw) || e.status == lift(EventStatus.Enriched))

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
          .filter(e => open(e))
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
      run(events.filter(e => open(e)).sortBy(_.occurredAt)(using Ord.desc).take(lift(limit)))
        .map(_.map(MapEventRow.toDomain))
        .provideEnvironment(env)

    // Conditional on the event being open, so a closed event keeps its first status and time, and a replayed request
    // changes nothing. Written as SQL: Quill's `update(_.field -> value)` does not parse on this Scala version
    def close(id: UUID, status: EventStatus, at: Instant): Task[Boolean] =
      run(
        sql"""UPDATE map_events SET status = ${lift(status)}, closed_at = ${lift(at)}
               WHERE id = ${lift(id)} AND status IN ('Raw', 'Enriched')""".as[Update[MapEventRow]]
      )
        .map(_ > 0)
        .provideEnvironment(env)

    // Served by idx_map_events_open (V6), oldest first
    def findExpired(eventType: EventType, cutoff: Instant, limit: Int): Task[List[MapEvent]] =
      run(
        events
          .filter(e => e.eventType == lift(eventType) && e.createdAt < lift(cutoff) && open(e))
          .sortBy(_.createdAt)
          .take(lift(limit))
      )
        .map(_.map(MapEventRow.toDomain))
        .provideEnvironment(env)
