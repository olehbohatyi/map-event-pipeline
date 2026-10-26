package com.github.olehbohatyi.mep.domain

import zio.json.JsonCodec
import zio.schema.{ derived, Schema }

final case class GeoPoint(lat: Double, lon: Double) derives JsonCodec, Schema

object GeoPoint:

  private val EarthRadiusKm = 6371.0088

  // Great-circle distance (haversine)
  def distanceKm(a: GeoPoint, b: GeoPoint): Double =
    val dLat = math.toRadians(b.lat - a.lat)
    val dLon = math.toRadians(b.lon - a.lon)
    val h    = math.pow(math.sin(dLat / 2), 2) +
      math.cos(math.toRadians(a.lat)) * math.cos(math.toRadians(b.lat)) * math.pow(math.sin(dLon / 2), 2)
    2 * EarthRadiusKm * math.asin(math.min(1.0, math.sqrt(h)))
