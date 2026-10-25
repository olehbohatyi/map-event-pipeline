package com.github.olehbohatyi.mep.pipe

import zio.*

import com.github.olehbohatyi.mep.domain.{ EventStatus, MapEvent }
import com.github.olehbohatyi.mep.geo.RegionResolver

trait EnrichmentService:
  def enrich(event: MapEvent): Task[MapEvent]

object EnrichmentService:

  val live: URLayer[RegionResolver, EnrichmentService] =
    ZLayer.fromFunction(EnrichmentServiceLive(_))

  final private class EnrichmentServiceLive(regions: RegionResolver) extends EnrichmentService:

    def enrich(event: MapEvent): Task[MapEvent] =
      regions
        .resolve(event.location)
        .map(region => event.copy(region = Some(region), status = EventStatus.Enriched))
