package com.github.olehbohatyi.mep.gate

import zio.*
import zio.test.*

import java.time.Instant
import java.util.UUID

import com.github.olehbohatyi.mep.domain.{ GeoPoint, MapEvent }
import com.github.olehbohatyi.mep.geo.RegionResolver
import com.github.olehbohatyi.mep.stores.*
import com.github.olehbohatyi.mep.testkit.InMemoryEventRepo

object QueryServiceSpec extends ZIOSpecDefault:

  // Records what the service asks the trends table for
  final private class RecordingTrends(asked: Ref[List[(Instant, Option[String])]]) extends TrendsRepo:
    def findSince(since: Instant, region: Option[String]): Task[List[MapEventTrend]] =
      asked.update(_ :+ (since, region)).as(Nil)

  private object NoStats extends StatsRepo:
    def findAll: Task[List[MapEventStat]]                      = ZIO.succeed(Nil)
    def findByRegion(region: String): Task[List[MapEventStat]] = ZIO.succeed(Nil)

  private object NoRegions extends RegionResolver:
    def resolve(point: GeoPoint): Task[String] = ZIO.succeed("none")

  def spec = suite("QueryService")(
    test("hourly asks for the windows that start within the last hours, the current one included") {
      for
        asked  <- Ref.make(List.empty[(Instant, Option[String])])
        rows   <- Ref.make(Map.empty[UUID, MapEvent])
        query  <- ZIO
                    .service[QueryService]
                    .provide(
                      QueryService.live,
                      ZLayer.succeed[EventRepo](InMemoryEventRepo(rows)),
                      ZLayer.succeed[StatsRepo](NoStats),
                      ZLayer.succeed[TrendsRepo](RecordingTrends(asked)),
                      ZLayer.succeed[RegionResolver](NoRegions),
                    )
        _      <- TestClock.setTime(Instant.parse("2026-10-27T14:37:12Z"))
        _      <- query.hourly(None, 1)
        _      <- query.hourly(Some("Kyiv City"), 24)
        result <- asked.get
      yield assertTrue(
        result == List(
          Instant.parse("2026-10-27T14:00:00Z") -> None,
          Instant.parse("2026-10-26T15:00:00Z") -> Some("Kyiv City"),
        )
      )
    }
  )
