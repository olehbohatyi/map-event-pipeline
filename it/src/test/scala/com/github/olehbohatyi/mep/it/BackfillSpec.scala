package com.github.olehbohatyi.mep.it

import zio.*
import zio.test.*

import javax.sql.DataSource

import org.flywaydb.core.Flyway
import com.github.olehbohatyi.mep.stores.DataSourceLayer

// The upgrade path: a database that already ran the region seed (V3) and holds events enriched before
// the polygons existed, then gets the later migrations
object BackfillSpec extends ZIOSpecDefault:

  private def migrateTo(target: String): RIO[DataSource, Unit] =
    ZIO.serviceWithZIO[DataSource](ds =>
      ZIO
        .attemptBlocking(
          Flyway.configure().dataSource(ds).locations("classpath:db/migration").target(target).load().migrate()
        )
        .unit
    )

  private def exec(sql: String): RIO[DataSource, Unit] =
    ZIO.serviceWithZIO[DataSource](ds =>
      ZIO.attemptBlocking(scala.util.Using.resource(ds.getConnection)(c => c.createStatement().execute(sql): Unit))
    )

  private def scalar(sql: String): RIO[DataSource, String] =
    ZIO.serviceWithZIO[DataSource](ds =>
      ZIO.attemptBlocking(scala.util.Using.resource(ds.getConnection) { c =>
        val rs = c.createStatement().executeQuery(sql)
        rs.next()
        rs.getString(1)
      })
    )

  private def event(description: String, lat: Double, lon: Double, region: String): String =
    s"""INSERT INTO map_events (id, event_type, severity, status, lat, lon, description, region, occurred_at, created_at)
       |VALUES (gen_random_uuid(), 'Hazard', 'Low', 'Enriched', $lat, $lon, '$description', '$region', now(), now());""".stripMargin

  def spec = (suite("Upgrade path")(
    test("renames grid-zone regions of existing events and drops the statistics left behind") {
      for
        _       <- migrateTo("3")
        _       <- exec(
                     List(
                       event("kyiv", 50.4501, 30.5234, "zone_50_30"),
                       event("lviv", 49.8397, 24.0297, "zone_49_24"),
                       event("odesa", 46.4825, 30.7233, "Odesa Oblast"), // already named: untouched
                       event("warsaw", 52.2297, 21.0122, "zone_52_21"),  // no region covers it: keeps its zone
                       """INSERT INTO map_event_stats (region, event_type, count) VALUES
                         |  ('zone_50_30', 'Hazard', 2), ('zone_49_24', 'Hazard', 1),
                         |  ('zone_52_21', 'Hazard', 1), ('Odesa Oblast', 'Hazard', 3);""".stripMargin,
                     ).mkString("\n")
                   )
        _       <- migrateTo("latest")
        regions <- scalar("SELECT string_agg(description || '=' || region, ', ' ORDER BY description) FROM map_events")
        stats   <- scalar("SELECT string_agg(region, ', ' ORDER BY region) FROM map_event_stats")
      yield assertTrue(
        regions == "kyiv=Kyiv City, lviv=Lviv Oblast, odesa=Odesa Oblast, warsaw=zone_52_21",
        // zone_50_30 and zone_49_24 no longer have events; the others still do
        stats == "Odesa Oblast, zone_52_21",
      )
    }
  ) @@ TestAspect.withLiveClock).provideShared(Containers.postgres, DataSourceLayer.live)
