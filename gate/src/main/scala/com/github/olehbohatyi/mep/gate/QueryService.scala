package com.github.olehbohatyi.mep.gate

import zio.*

import com.github.olehbohatyi.mep.domain.GeoPoint
import com.github.olehbohatyi.mep.geo.RegionResolver
import com.github.olehbohatyi.mep.stores.{ EventRepo, MapEventStat, StatsRepo }

trait QueryService:
  def findNearby(center: GeoPoint, radiusKm: Double, limit: Int): Task[NearbyResponse]
  def recent(limit: Int): Task[RecentResponse]
  def stats(region: Option[String]): Task[List[MapEventStat]]

object QueryService:

  val live: URLayer[EventRepo & StatsRepo & RegionResolver, QueryService] =
    ZLayer.fromFunction(QueryServiceLive(_, _, _))

  final private class QueryServiceLive(
      events: EventRepo,
      stats: StatsRepo,
      regions: RegionResolver,
    ) extends QueryService:

    def findNearby(center: GeoPoint, radiusKm: Double, limit: Int): Task[NearbyResponse] =
      (regions.resolve(center) <&> events.findNearby(center, radiusKm, limit)).flatMap { (region, found) =>
        // Events stored before enrichment have no region yet
        ZIO
          .foreach(found)(e =>
            e.region.fold(regions.resolve(e.location).map(r => e.copy(region = Some(r))))(_ => ZIO.succeed(e))
          )
          .map(enriched => NearbyResponse(region, enriched.size, enriched))
      }

    def recent(limit: Int): Task[RecentResponse] =
      events.findRecent(limit).map(found => RecentResponse(found.size, found))

    def stats(region: Option[String]): Task[List[MapEventStat]] =
      region.fold(stats.findAll)(stats.findByRegion)
