package com.github.olehbohatyi.mep.it

import zio.*
import zio.test.*

import javax.sql.DataSource

import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.stores.*
import com.github.olehbohatyi.mep.testkit.Samples

object PostgresSpec extends ZIOSpecDefault:

  private def exec(sql: String): RIO[DataSource, Unit] =
    ZIO.serviceWithZIO[DataSource](ds =>
      ZIO.attemptBlocking(scala.util.Using.resource(ds.getConnection)(c => c.createStatement().execute(sql): Unit))
    )

  def spec = (suite("Postgres stores")(
    test("insert is idempotent by event id") {
      val event = Samples.event(status = EventStatus.Enriched, region = Some("zone_50_30"))
      for
        repo   <- ZIO.service[EventRepo]
        first  <- repo.insert(event)
        second <- repo.insert(event.copy(description = Some("changed")))
        found  <- repo.findById(event.id)
      yield assertTrue(first, !second, found.contains(event))
    },
    test("stats are read per region from map_event_stats") {
      for
        _      <- exec(
                    """INSERT INTO map_event_stats (region, event_type, count) VALUES
                      |('zone_1_2', 'Hazard', 3), ('zone_1_2', 'Sign', 1), ('zone_9_9', 'Traffic', 7)""".stripMargin
                  )
        repo   <- ZIO.service[StatsRepo]
        region <- repo.findByRegion("zone_1_2")
        all    <- repo.findAll
      yield assertTrue(
        region.map(s => s.eventType -> s.count) == List(EventType.Hazard -> 3L, EventType.Sign -> 1L),
        all.size == 3,
      )
    },
  ) @@ TestAspect.beforeAll(Migrations.run.orDie) @@ TestAspect.sequential @@ TestAspect.withLiveClock)
    .provideShared(Containers.postgres, DataSourceLayer.live, EventRepo.live, StatsRepo.live)
