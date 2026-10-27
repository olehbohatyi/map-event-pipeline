package com.github.olehbohatyi.mep.gate

import zio.*
import zio.kafka.consumer.{ Consumer, ConsumerSettings, Subscription }
import zio.kafka.serde.Serde
import zio.stream.ZStream

import com.github.olehbohatyi.mep.broker.EventCodec
import com.github.olehbohatyi.mep.config.KafkaConfig
import com.github.olehbohatyi.mep.domain.{ GeoPoint, MapEvent }

// minLon,minLat,maxLon,maxLat — the order Leaflet's `getBounds().toBBoxString()` and most map tools use
final case class BoundingBox(minLon: Double, minLat: Double, maxLon: Double, maxLat: Double):
  def contains(p: GeoPoint): Boolean = p.lon >= minLon && p.lon <= maxLon && p.lat >= minLat && p.lat <= maxLat

object BoundingBox:

  def parse(s: String): Either[String, BoundingBox] =
    // -1 keeps trailing empty fields, so "1,2,3,4," is five fields; NaN and infinities would pass every range check
    s.split(",", -1).map(_.trim.toDoubleOption.filter(_.isFinite)).toList match
      case List(Some(minLon), Some(minLat), Some(maxLon), Some(maxLat)) =>
        val box = BoundingBox(minLon, minLat, maxLon, maxLat)
        if List(minLon, maxLon).exists(v => v < -180 || v > 180) then Left("bbox longitudes must be within [-180, 180]")
        else if List(minLat, maxLat).exists(v => v < -90 || v > 90) then Left("bbox latitudes must be within [-90, 90]")
        else if minLon > maxLon || minLat > maxLat then Left("bbox minimum must not exceed its maximum")
        else Right(box)
      case _                                                            =>
        Left("bbox must be four numbers: minLon,minLat,maxLon,maxLat")

// Events as the pipe finishes them, pushed to any number of listeners (the live map)
trait LiveFeed:
  // Events published while the stream runs, optionally only those inside the area
  def subscribe(area: Option[BoundingBox]): ZStream[Any, Nothing, MapEvent]
  // Current number of running subscriptions
  def subscribers: UIO[Int]
  // Ends every subscription. Called before the HTTP server's graceful shutdown, which otherwise waits for the
  // endless streams until its timeout
  def close: UIO[Unit]

object LiveFeed:

  // How far a subscriber may fall behind before it starts losing its oldest events
  val BufferSize = 1024

  /** Fans `source` out to every subscriber through a sliding hub: a subscriber that cannot keep up (a slow or stalled
    * client) drops its oldest events instead of slowing the source or the other subscribers. The source is read in the
    * background for the lifetime of the scope and restarted with backoff when it fails.
    */
  def fromSource(source: ZStream[Any, Throwable, MapEvent]): URIO[Scope, LiveFeed] =
    for
      hub   <- Hub.sliding[MapEvent](BufferSize)
      count <- Ref.make(0)
      // Retries after 1, 2, 4, 8, 16 s, then every 30 s for as long as the source keeps failing
      _     <- source
                 .tapErrorCause(cause => ZIO.logWarningCause("live feed source failed, restarting", cause))
                 .retry(Schedule.exponential(1.second).upTo(30.seconds).andThen(Schedule.spaced(30.seconds)))
                 .runForeach(hub.publish)
                 .forkScoped
      _     <- ZIO.addFinalizer(hub.shutdown)
    yield LiveFeedLive(hub, count)

  /** Tails the enriched topic. Every gate instance uses its own consumer group and starts at the newest offset, so each
    * instance sees every event and none replays history; nothing is committed. The pipe may publish an event twice
    * after a retry, so listeners key events by id.
    */
  val live: RLayer[KafkaConfig, LiveFeed] =
    ZLayer.scoped {
      for
        cfg      <- ZIO.service[KafkaConfig]
        group    <- Random.nextUUID.map(id => s"mep-gate-live-$id")
        consumer <- Consumer.make(
                      ConsumerSettings(cfg.servers)
                        .withGroupId(group)
                        .withOffsetRetrieval(Consumer.OffsetRetrieval.Auto(Consumer.AutoOffsetStrategy.Latest))
                    )
        source    = consumer
                      .plainStream(Subscription.topics(cfg.topics.enriched), Serde.string, Serde.string)
                      .mapZIO(record =>
                        EventCodec.decode(record.value) match
                          case Right(event) => ZIO.some(event)
                          case Left(err)    => ZIO.logWarning(s"skipping unreadable enriched record: $err").as(None)
                      )
                      .collectSome
        feed     <- fromSource(source)
      yield feed
    }

  final private class LiveFeedLive(hub: Hub[MapEvent], count: Ref[Int]) extends LiveFeed:

    // Counted only once the hub subscription exists, so a subscriber that is counted receives what follows
    def subscribe(area: Option[BoundingBox]): ZStream[Any, Nothing, MapEvent] =
      ZStream.unwrapScoped(
        for
          queue <- hub.subscribe
          _     <- ZIO.acquireRelease(count.update(_ + 1))(_ => count.update(_ - 1))
        yield ZStream.fromQueue(queue).filter(e => area.forall(_.contains(e.location)))
      )

    def subscribers: UIO[Int] = count.get

    def close: UIO[Unit] = hub.shutdown
