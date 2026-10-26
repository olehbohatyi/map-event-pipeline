package com.github.olehbohatyi.mep.stores

import zio.*

import javax.sql.DataSource

import com.github.olehbohatyi.mep.domain.{ GeoPoint, Region }
import io.getquill.*

trait RegionRepo:
  // The region whose polygon contains the point; None for points outside every known region (e.g. at sea)
  def findContaining(point: GeoPoint): Task[Option[Region]]

object RegionRepo:

  val live: URLayer[DataSource, RegionRepo] =
    ZLayer.fromFunction(RegionRepoLive(_))

  final private class RegionRepoLive(ds: DataSource) extends RegionRepo:

    import QuillContext.*

    // geom is not mapped: polygons stay in the database and are only used by the spatial predicate below
    inline private def regions = quote(querySchema[Region]("regions"))

    private val env = ZEnvironment(ds)

    def findContaining(point: GeoPoint): Task[Option[Region]] =
      val lat = point.lat
      val lon = point.lon
      run(
        regions
          .filter(_ => sql"ST_Covers(geom, ST_SetSRID(ST_MakePoint(${lift(lon)}, ${lift(lat)}), 4326))".asCondition)
          // A point exactly on a shared border is covered by both neighbours; pick one deterministically
          .sortBy(_.code)
          .take(1)
      )
        .map(_.headOption)
        .provideEnvironment(env)
