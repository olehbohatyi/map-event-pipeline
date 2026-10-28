package com.github.olehbohatyi.mep.stores

import zio.*
import zio.json.JsonCodec
import zio.schema.{ derived, Schema }

import java.time.Instant
import javax.sql.DataSource

import com.github.olehbohatyi.mep.domain.EventType
import io.getquill.*

// Row of map_event_trends, written by the stat module's streaming job (TrendsApp)
final case class MapEventTrend(
    windowStart: Instant,
    region: String,
    eventType: EventType,
    count: Long,
    updatedAt: Instant,
  ) derives JsonCodec,
      Schema

trait TrendsRepo:
  // Hourly counts whose window starts at or after `since`, oldest first, optionally for one region
  def findSince(since: Instant, region: Option[String]): Task[List[MapEventTrend]]

object TrendsRepo:

  val live: URLayer[DataSource, TrendsRepo] =
    ZLayer.fromFunction(TrendsRepoLive(_))

  final private class TrendsRepoLive(ds: DataSource) extends TrendsRepo:

    import QuillContext.{ *, given }

    inline private def trends = quote(querySchema[MapEventTrend]("map_event_trends"))

    private val env = ZEnvironment(ds)

    def findSince(since: Instant, region: Option[String]): Task[List[MapEventTrend]] =
      val recent = quote(trends.filter(_ => sql"window_start >= ${lift(since)}".asCondition))
      region
        .fold(run(recent.sortBy(t => (t.windowStart, t.region, t.eventType))))(r =>
          run(recent.filter(_.region == lift(r)).sortBy(t => (t.windowStart, t.eventType)))
        )
        .provideEnvironment(env)
