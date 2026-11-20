package com.github.olehbohatyi.mep.gate

import zio.*
import zio.test.*

import java.time.Instant
import java.util.UUID

import com.github.olehbohatyi.mep.broker.EventCodec
import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.testkit.*

object ResolutionServiceSpec extends ZIOSpecDefault:

  private val now = Instant.parse("2026-11-20T12:00:00Z")

  private def stored(event: MapEvent): URIO[InMemoryEventRepo, MapEvent] =
    ZIO.serviceWithZIO[InMemoryEventRepo](_.insert(event).orDie).as(event)

  def spec = suite("ResolutionService")(
    test("asks the pipe to resolve a stored open event, on the status topic keyed by its id") {
      for
        _         <- TestClock.setTime(now)
        event     <- stored(Samples.event(status = EventStatus.Enriched))
        result    <- ZIO.serviceWithZIO[ResolutionService](_.resolve(event.id))
        sent      <- ZIO.serviceWithZIO[RecordingProducer](_.allRaw)
        unchanged <- ZIO.serviceWithZIO[InMemoryEventRepo](_.findById(event.id))
      yield assertTrue(
        result == ResolveResult.Requested(event.id),
        sent.map(p => (p.topic, p.key)) == Chunk(("status", Some(event.id.toString))),
        sent.map(p => EventCodec.decodeChange(p.value)) ==
          Chunk(Right(StatusChange(event.id, EventStatus.Resolved, now))),
        // The gate only asks; the pipe changes the event
        unchanged.exists(_.status == EventStatus.Enriched),
      )
    },
    test("publishes nothing for an event that is not stored or closed already") {
      for
        expired <- stored(Samples.event(status = EventStatus.Expired))
        missing  = UUID.randomUUID()
        svc     <- ZIO.service[ResolutionService]
        closed  <- svc.resolve(expired.id)
        absent  <- svc.resolve(missing)
        sent    <- ZIO.serviceWithZIO[RecordingProducer](_.allRaw)
      yield assertTrue(
        closed == ResolveResult.AlreadyClosed(expired.id, EventStatus.Expired),
        absent == ResolveResult.NotFound(missing),
        sent.isEmpty,
      )
    },
  ).provide(
    RecordingProducer.layer,
    InMemoryEventRepo.layer,
    ZLayer.succeed(TestKafka.config()),
    ResolutionService.live,
  )
