package com.github.olehbohatyi.mep.gate

import zio.*
import zio.stream.ZStream
import zio.test.*

import com.github.olehbohatyi.mep.domain.{ GeoPoint, MapEvent }
import com.github.olehbohatyi.mep.testkit.Samples

object LiveFeedSpec extends ZIOSpecDefault:

  private val kyiv = GeoPoint(50.45, 30.52)
  private val lviv = GeoPoint(49.84, 24.03)

  // A feed over an in-memory source
  private val feed: URIO[Scope, (Queue[MapEvent], LiveFeed)] =
    for
      source <- Queue.unbounded[MapEvent]
      feed   <- LiveFeed.fromSource(ZStream.fromQueue(source))
    yield (source, feed)

  def spec = suite("LiveFeed")(
    test("every subscriber gets every event; an area keeps only the events inside it") {
      val inKyiv = Samples.event(location = kyiv)
      val inLviv = Samples.event(location = lviv)
      ZIO.scoped {
        for
          (source, live) <- feed
          all            <- live.subscribe(None).take(2).runCollect.fork
          kyivOnly       <- live.subscribe(Some(BoundingBox(30, 50, 31, 51))).take(1).runCollect.fork
          _              <- live.subscribers.repeatUntil(_ == 2)
          _              <- source.offerAll(List(inLviv, inKyiv))
          got            <- all.join
          gotKyiv        <- kyivOnly.join
        yield assertTrue(got.map(_.id) == Chunk(inLviv.id, inKyiv.id), gotKyiv.map(_.id) == Chunk(inKyiv.id))
      }
    },
    test("a subscriber that stops reading holds back neither the source nor the others") {
      val events = (1 to LiveFeed.BufferSize * 5).map(_ => Samples.event()).toList
      ZIO.scoped {
        for
          (source, live) <- feed
          // Takes the first event and then never asks for another
          _              <- live.subscribe(None).mapZIO(_ => ZIO.never).runDrain.fork
          fast           <- live.subscribe(None).takeUntil(_.id == events.last.id).runCount.fork
          _              <- live.subscribers.repeatUntil(_ == 2)
          _              <- source.offerAll(events)
          count          <- fast.join
          // The source was read to the end (the feed waits on it again)
          drained        <- source.isEmpty
        yield assertTrue(count > 0, drained)
      }
    },
    test("a subscription is released when its stream ends") {
      ZIO.scoped {
        for
          (source, live) <- feed
          one            <- live.subscribe(None).take(1).runDrain.fork
          _              <- live.subscribers.repeatUntil(_ == 1)
          _              <- source.offer(Samples.event())
          _              <- one.join
          after          <- live.subscribers
        yield assertTrue(after == 0)
      }
    },
    test("close ends every subscription instead of leaving it open") {
      ZIO.scoped {
        for
          (_, live) <- feed
          streams   <- ZIO.foreach(1 to 3)(_ => live.subscribe(None).runCollect.fork)
          _         <- live.subscribers.repeatUntil(_ == 3)
          _         <- live.close
          ended     <- ZIO.foreach(streams)(_.join)
          after     <- live.subscribers
        yield assertTrue(ended.forall(_.isEmpty), after == 0)
      }
    },
    test("a failing source is restarted, so the feed recovers once Kafka is back") {
      val event = Samples.event()
      ZIO.scoped {
        for
          attempts <- Ref.make(0)
          source    = ZStream.fromZIO(attempts.getAndUpdate(_ + 1)).flatMap {
                        case 0 => ZStream.fail(RuntimeException("broker unavailable"))
                        case _ => ZStream.succeed(event) ++ ZStream.never
                      }
          live     <- LiveFeed.fromSource(source)
          got      <- live.subscribe(None).take(1).runCollect.fork
          _        <- live.subscribers.repeatUntil(_ == 1)
          // The first retry waits a second
          _        <- TestClock.adjust(2.seconds)
          events   <- got.join
        yield assertTrue(events.map(_.id) == Chunk(event.id))
      }
    },
    suite("BoundingBox")(
      test("parses minLon,minLat,maxLon,maxLat and tests containment inclusively") {
        val box = BoundingBox.parse(" 30, 50 ,31,51")
        assertTrue(
          box == Right(BoundingBox(30, 50, 31, 51)),
          box.exists(_.contains(kyiv)),
          box.exists(!_.contains(lviv)),
          box.exists(_.contains(GeoPoint(51, 31))),
        )
      },
      test("rejects malformed and impossible boxes") {
        val errors = List("30,50,31", "a,50,31,51", "30,50,31,51,1", "", "-181,0,0,1", "0,-91,1,0", "31,50,30,51")
          .map(BoundingBox.parse)
        assertTrue(errors.forall(_.isLeft))
      },
    ),
  ) @@ TestAspect.timeout(30.seconds)
