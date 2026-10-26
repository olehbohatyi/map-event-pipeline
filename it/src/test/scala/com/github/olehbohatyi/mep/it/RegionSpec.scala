package com.github.olehbohatyi.mep.it

import zio.*
import zio.test.*

import javax.sql.DataSource

import com.github.olehbohatyi.mep.domain.GeoPoint
import com.github.olehbohatyi.mep.stores.*

// Runs the real migrations, including the Natural Earth seed (V3), against PostGIS
object RegionSpec extends ZIOSpecDefault:

  private def scalar(sql: String): RIO[DataSource, String] =
    ZIO.serviceWithZIO[DataSource](ds =>
      ZIO.attemptBlocking(scala.util.Using.resource(ds.getConnection) { c =>
        val rs = c.createStatement().executeQuery(sql)
        rs.next()
        rs.getString(1)
      })
    )

  // City → expected region; None for places no region polygon covers
  private val places: List[(String, GeoPoint, Option[String])] = List(
    ("Kyiv centre", GeoPoint(50.4501, 30.5234), Some("Kyiv City")),
    ("Boryspil", GeoPoint(50.35, 30.9), Some("Kyiv Oblast")),
    ("Lviv", GeoPoint(49.8397, 24.0297), Some("Lviv Oblast")),
    ("Odesa", GeoPoint(46.4825, 30.7233), Some("Odesa Oblast")),
    ("Kharkiv", GeoPoint(49.9935, 36.2304), Some("Kharkiv Oblast")),
    ("Dnipro", GeoPoint(48.4647, 35.0462), Some("Dnipropetrovsk Oblast")),
    ("Uzhhorod", GeoPoint(48.6208, 22.2879), Some("Zakarpattia Oblast")),
    ("Donetsk", GeoPoint(48.0159, 37.8028), Some("Donetsk Oblast")),
    ("Luhansk", GeoPoint(48.574, 39.3078), Some("Luhansk Oblast")),
    ("Simferopol", GeoPoint(44.9521, 34.1024), Some("Autonomous Republic of Crimea")),
    ("Sevastopol (land)", GeoPoint(44.55, 33.6), Some("Sevastopol City")),
    ("Warsaw", GeoPoint(52.2297, 21.0122), None),
    ("Black Sea", GeoPoint(44.0, 31.0), None),
  )

  def spec = (suite("Regions (PostGIS)")(
    test("the seed holds all 27 Ukrainian first-level regions") {
      for
        count    <- scalar("SELECT count(*) FROM regions WHERE country = 'UA'")
        crimea   <- scalar("SELECT count(*) FROM regions WHERE code IN ('UA-43', 'UA-40')")
        invalid  <- scalar("SELECT count(*) FROM regions WHERE NOT ST_IsValid(geom)")
        overlaps <-
          scalar("SELECT count(*) FROM regions a JOIN regions b ON a.code < b.code AND ST_Overlaps(a.geom, b.geom)")
      yield assertTrue(count == "27", crimea == "2", invalid == "0", overlaps == "0")
    },
    test("finds the region containing a point, or none outside the known regions") {
      for
        repo  <- ZIO.service[RegionRepo]
        found <- ZIO.foreach(places)((name, point, _) => repo.findContaining(point).map(r => name -> r.map(_.name)))
        wanted = places.map((name, _, expected) => name -> expected)
      yield assertTrue(found == wanted)
    },
    test("reports the ISO code and country with the region") {
      for region <- ZIO.serviceWithZIO[RegionRepo](_.findContaining(GeoPoint(49.8397, 24.0297)))
      yield assertTrue(region.exists(r => r.code == "UA-46" && r.country == "UA"))
    },
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock @@ TestAspect.beforeAll(Migrations.run.orDie))
    .provideShared(Containers.postgres, DataSourceLayer.live, RegionRepo.live)
