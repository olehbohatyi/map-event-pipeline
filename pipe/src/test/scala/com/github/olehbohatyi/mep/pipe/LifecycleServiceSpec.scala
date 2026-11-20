package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.concurrent.CountdownLatch
import zio.test.*

import java.time.Instant
import java.util.UUID

import com.github.olehbohatyi.mep.broker.ProducerService
import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.stores.EventRepo
import com.github.olehbohatyi.mep.testkit.*

object LifecycleServiceSpec extends ZIOSpecDefault:

  private val now = Instant.parse("2026-11-20T12:00:00Z")

  private def stored(event: MapEvent): URIO[InMemoryEventRepo, MapEvent] =
    ZIO.serviceWithZIO[InMemoryEventRepo](_.insert(event).orDie).as(event)

  private def find(id: UUID): URIO[InMemoryEventRepo, Option[MapEvent]] =
    ZIO.serviceWithZIO[InMemoryEventRepo](_.findById(id).orDie)

  // Received `ago` before now
  private def received(eventType: EventType, ago: Duration): MapEvent =
    val at = now.minus(ago)
    Samples.event(eventType = eventType, status = EventStatus.Enriched).copy(occurredAt = at, createdAt = at)

  private val published: URIO[RecordingProducer, Chunk[(String, MapEvent)]] =
    ZIO.serviceWithZIO[RecordingProducer](_.all)

  def spec = suite("LifecycleService")(
    test("resolves an open event and publishes it to the closed topic") {
      val at = now.minusSeconds(5)
      for
        event   <- stored(Samples.event(status = EventStatus.Enriched))
        outcome <- ZIO.serviceWithZIO[LifecycleService](_.apply(StatusChange(event.id, EventStatus.Resolved, at)))
        after   <- find(event.id)
        sent    <- published
      yield assertTrue(
        outcome == CloseOutcome.Closed(EventStatus.Resolved),
        after.exists(e => e.status == EventStatus.Resolved && e.closedAt.contains(at)),
        sent == Chunk("closed" -> event.copy(status = EventStatus.Resolved, closedAt = Some(at))),
      )
    },
    // A replayed request, or one for an event that expired meanwhile: the first close stands
    test("leaves a closed event as it is, and publishes it again in case an earlier publish was lost") {
      val first = now.minusSeconds(60)
      for
        event   <- stored(Samples.event(status = EventStatus.Enriched))
        svc     <- ZIO.service[LifecycleService]
        _       <- svc.apply(StatusChange(event.id, EventStatus.Expired, first))
        outcome <- svc.apply(StatusChange(event.id, EventStatus.Resolved, now))
        after   <- find(event.id)
        sent    <- published
      yield assertTrue(
        outcome == CloseOutcome.AlreadyClosed(EventStatus.Expired),
        after.exists(e => e.status == EventStatus.Expired && e.closedAt.contains(first)),
        sent.map(_._2.status) == Chunk(EventStatus.Expired, EventStatus.Expired),
      )
    },
    test("ignores a change to an open status and one for an event that is not stored") {
      for
        event   <- stored(Samples.event(status = EventStatus.Enriched))
        svc     <- ZIO.service[LifecycleService]
        reopen  <- svc.apply(StatusChange(event.id, EventStatus.Enriched, now))
        missing <- svc.apply(StatusChange(UUID.randomUUID(), EventStatus.Resolved, now))
        sent    <- published
      yield assertTrue(
        reopen.isInstanceOf[CloseOutcome.Ignored],
        missing.isInstanceOf[CloseOutcome.Ignored],
        sent.isEmpty,
      )
    },
    test("expires open events older than their type's lifetime, and only those") {
      for
        _          <- TestClock.setTime(now)
        oldTraffic <- stored(received(EventType.Traffic, 3.hours))
        newTraffic <- stored(received(EventType.Traffic, 1.hour))
        // Three hours is past Traffic's two but within Hazard's six
        hazard     <- stored(received(EventType.Hazard, 3.hours))
        resolved   <- stored(received(EventType.Traffic, 5.hours).copy(status = EventStatus.Resolved))
        expired    <- ZIO.serviceWithZIO[LifecycleService](_.expireDue)
        statuses   <- ZIO.foreach(List(oldTraffic, newTraffic, hazard, resolved))(e => find(e.id).map(_.map(_.status)))
        sent       <- published
      yield assertTrue(
        expired == 1,
        statuses == List(EventStatus.Expired, EventStatus.Enriched, EventStatus.Enriched, EventStatus.Resolved).map(
          Some(_)
        ),
        sent == Chunk("closed" -> oldTraffic.copy(status = EventStatus.Expired, closedAt = Some(now))),
      )
    },
    // Closed first and the publish lost, no later sweep would find the event: it would stay on the live map
    test("an event whose publish fails stays open, and the next sweep expires and publishes it") {
      for
        _       <- TestClock.setTime(now)
        event   <- stored(received(EventType.Traffic, 3.hours))
        svc     <- ZIO.service[LifecycleService]
        _       <- ZIO.serviceWithZIO[RecordingProducer](_.failNext(1))
        failed  <- svc.expireDue.exit
        between <- find(event.id)
        expired <- svc.expireDue
        after   <- find(event.id)
        sent    <- published
      yield assertTrue(
        failed.isFailure,
        between.exists(_.status == EventStatus.Enriched),
        expired == 1,
        after.exists(_.status == EventStatus.Expired),
        sent.map((topic, e) => topic -> e.status) == Chunk("closed" -> EventStatus.Expired),
      )
    },
    // One event at a time, each waiting for the broker and Postgres, a backlog after downtime took minutes
    test("expires the events of a batch concurrently") {
      for
        _       <- TestClock.setTime(now)
        _       <- ZIO.foreachDiscard(1 to LifecycleService.Parallelism)(_ => stored(received(EventType.Traffic, 3.hours)))
        // Each publish waits until every event of the batch is being published: one at a time, the first never returns
        latch   <- CountdownLatch.make(LifecycleService.Parallelism)
        repo    <- ZIO.service[InMemoryEventRepo]
        kafka    = TestKafka.config()
        producer = new ProducerService:
                     def publish(topic: String, event: MapEvent): Task[Unit] = latch.countDown *> latch.await
                     def publishRaw(topic: String, key: Option[String], value: String, headers: Map[String, String])
                         : Task[Unit]                                        = ZIO.unit
        expire   = ZIO
                     .serviceWithZIO[LifecycleService](_.expireDue)
                     .provide(
                       ZLayer.succeed[EventRepo](repo),
                       ZLayer.succeed[ProducerService](producer),
                       ZLayer.succeed(kafka),
                       ZLayer.succeed(TestLifecycle.config),
                       ZLayer.succeed(TestLifecycle.pool()),
                       LifecycleService.live,
                     )
        // The sweep runs on the test clock; only the timeout is real time
        expired <- expire.raceFirst(Live.live(ZIO.sleep(10.seconds)) *> ZIO.fail(RuntimeException("one at a time")))
      yield assertTrue(expired == LifecycleService.Parallelism)
    },
    test("leaves two connections of the Postgres pool to the stream") {
      assertTrue(
        LifecycleService.parallelism(10) == 8,
        LifecycleService.parallelism(30) == LifecycleService.Parallelism,
        LifecycleService.parallelism(4) == 2,
        LifecycleService.parallelism(2) == 1,
      )
    },
    test("works through a backlog larger than one batch") {
      val backlog = List.fill(LifecycleService.Batch + 3)(received(EventType.Sign, 31.days))
      for
        _       <- TestClock.setTime(now)
        _       <- ZIO.foreachDiscard(backlog)(stored)
        expired <- ZIO.serviceWithZIO[LifecycleService](_.expireDue)
        again   <- ZIO.serviceWithZIO[LifecycleService](_.expireDue)
      yield assertTrue(expired == backlog.size, again == 0)
    },
  ).provide(
    InMemoryEventRepo.layer,
    RecordingProducer.layer,
    ZLayer.succeed(TestKafka.config()),
    ZLayer.succeed(TestLifecycle.config),
    ZLayer.succeed(TestLifecycle.pool()),
    LifecycleService.live,
  )
