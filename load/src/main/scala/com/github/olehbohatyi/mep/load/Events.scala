package com.github.olehbohatyi.mep.load

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import scala.jdk.CollectionConverters.*

// Random events around Ukrainian cities, as Gatling feeder records
object Events:

  private val cities     = Vector(
    (50.45, 30.52),
    (49.84, 24.03),
    (49.99, 36.23),
    (46.48, 30.72),
    (48.46, 35.05),
    (47.84, 35.14),
    (49.23, 28.47),
    (49.59, 34.55),
    (51.50, 31.29),
    (48.62, 22.29),
  )
  private val types      = Vector("Hazard", "Hazard", "Hazard", "Sign", "Traffic", "Traffic")
  private val severities = Vector("Low", "Low", "Low", "Medium", "Medium", "High", "Critical")

  // About one request in ten reuses a key from a small pool, as a client retrying would: the gate answers 200
  private val DuplicateShare = 0.1
  private val DuplicateKeys  = 100

  def feeder: java.util.Iterator[java.util.Map[String, AnyRef]] =
    Iterator.continually(next()).asJava

  private def next(): java.util.Map[String, AnyRef] =
    val r          = ThreadLocalRandom.current()
    val (lat, lon) = cities(r.nextInt(cities.size))
    val pointLat   = f"${lat + r.nextDouble(-0.15, 0.15)}%.4f"
    val pointLon   = f"${lon + r.nextDouble(-0.15, 0.15)}%.4f"
    val key        =
      if r.nextDouble() < DuplicateShare then s"load-retry-${r.nextInt(DuplicateKeys)}" else UUID.randomUUID().toString
    val body       =
      s"""{"eventType":"${types(r.nextInt(types.size))}","severity":"${severities(r.nextInt(severities.size))}",""" +
        s""""location":{"lat":$pointLat,"lon":$pointLon},"description":"load test"}"""
    Map[String, AnyRef]("lat" -> pointLat, "lon" -> pointLon, "key" -> key, "body" -> body).asJava
