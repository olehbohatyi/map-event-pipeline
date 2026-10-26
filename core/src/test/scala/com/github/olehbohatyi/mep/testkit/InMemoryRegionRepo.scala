package com.github.olehbohatyi.mep.testkit

import zio.*

import com.github.olehbohatyi.mep.domain.{ GeoPoint, Region }
import com.github.olehbohatyi.mep.stores.RegionRepo

// Regions defined by a containment predicate instead of a polygon; counts lookups so tests can see cache hits
final class InMemoryRegionRepo(regions: List[(Region, GeoPoint => Boolean)], lookups: Ref[Int]) extends RegionRepo:
  def findContaining(point: GeoPoint): Task[Option[Region]] =
    lookups.update(_ + 1).as(regions.collectFirst { case (region, contains) if contains(point) => region })

  def lookupCount: UIO[Int] = lookups.get

object InMemoryRegionRepo:
  def layer(regions: (Region, GeoPoint => Boolean)*): ULayer[InMemoryRegionRepo] =
    ZLayer(Ref.make(0).map(InMemoryRegionRepo(regions.toList, _)))
