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

object EventRepo:

  // Upper bound of rows fetched from the bounding box before the exact distance filter
  private val MaxCandidates = 5000

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

    def findNearby(center: GeoPoint, radiusKm: Double, limit: Int): Task[List[MapEvent]] =
      val box = BoundingBox.around(center, radiusKm)
      run(
        events
          .filter(r =>
            r.lat >= lift(box.minLat) && r.lat <= lift(box.maxLat) &&
            r.lon >= lift(box.minLon) && r.lon <= lift(box.maxLon)
          )
          .take(lift(MaxCandidates))
      )
        .map(
          _.map(MapEventRow.toDomain)
            .map(e => e -> GeoPoint.distanceKm(center, e.location))
            .filter(_._2 <= radiusKm)
            .sortBy(_._2)
            .take(limit)
            .map(_._1)
        )
        .provideEnvironment(env)

// Lat/lon box enclosing a circle; the lon span widens with latitude
final case class BoundingBox(minLat: Double, maxLat: Double, minLon: Double, maxLon: Double)

object BoundingBox:

  private val KmPerDegree = 111.32

  def around(center: GeoPoint, radiusKm: Double): BoundingBox =
    val dLat   = radiusKm / KmPerDegree
    val cosLat = math.cos(math.toRadians(center.lat))
    val dLon   = if cosLat < 1e-6 then 180.0 else math.min(180.0, radiusKm / (KmPerDegree * cosLat))
    BoundingBox(
      minLat = math.max(-90.0, center.lat - dLat),
      maxLat = math.min(90.0, center.lat + dLat),
      minLon = center.lon - dLon,
      maxLon = center.lon + dLon,
    )
