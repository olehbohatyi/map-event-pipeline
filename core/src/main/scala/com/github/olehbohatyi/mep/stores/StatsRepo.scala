package com.github.olehbohatyi.mep.stores

import zio.*
import zio.json.JsonEncoder

import java.time.Instant
import javax.sql.DataSource

import com.github.olehbohatyi.mep.domain.EventType
import io.getquill.*

// Row of map_event_stats, written by the stat job
final case class MapEventStat(
    region: String,
    eventType: EventType,
    count: Long,
    updatedAt: Instant,
  ) derives JsonEncoder

trait StatsRepo:
  def findAll: Task[List[MapEventStat]]
  def findByRegion(region: String): Task[List[MapEventStat]]

object StatsRepo:

  val live: URLayer[DataSource, StatsRepo] =
    ZLayer.fromFunction(StatsRepoLive(_))

  final private class StatsRepoLive(ds: DataSource) extends StatsRepo:

    import QuillContext.{ *, given }

    inline private def stats = quote(querySchema[MapEventStat]("map_event_stats"))

    private val env = ZEnvironment(ds)

    def findAll: Task[List[MapEventStat]] =
      run(stats.sortBy(s => (s.region, s.eventType))).provideEnvironment(env)

    def findByRegion(region: String): Task[List[MapEventStat]] =
      run(stats.filter(_.region == lift(region)).sortBy(_.eventType)).provideEnvironment(env)
