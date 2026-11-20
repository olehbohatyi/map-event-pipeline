package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.test.*

import com.github.olehbohatyi.mep.broker.EventCodec
import com.github.olehbohatyi.mep.domain.{ EventStatus, MapEvent, StatusChange }
import com.github.olehbohatyi.mep.testkit.*

object RecordHandlerSpec extends ZIOSpecDefault:

  // EventHandler double: fails the first `failures` calls, then processes
  final class StubHandler(calls: Ref[Int], failures: Int) extends EventHandler:
    def handle(event: MapEvent): Task[Outcome] =
      calls
        .getAndUpdate(_ + 1)
        .flatMap(n =>
          if n < failures then ZIO.fail(RuntimeException("db down")) else ZIO.succeed(Outcome.Processed("hazards"))
        )
    def callCount: UIO[Int]                    = calls.get

  private def stub(failures: Int) =
    ZLayer(Ref.make(0).map(StubHandler(_, failures)))

  // LifecycleService double: records the changes it is given; fails the first `failures` of them
  final class StubLifecycle(changes: Ref[Chunk[StatusChange]], failures: Ref[Int]) extends LifecycleService:
    def apply(change: StatusChange): Task[CloseOutcome] =
      failures.getAndUpdate(n => math.max(0, n - 1)).flatMap { n =>
        if n > 0 then ZIO.fail(RuntimeException("db down"))
        else changes.update(_ :+ change).as(CloseOutcome.Closed(change.status))
      }
    def expireDue: Task[Int]                            = ZIO.succeed(0)
    def sweep: UIO[Nothing]                             = ZIO.never
    def seen: UIO[Chunk[StatusChange]]                  = changes.get

  private def lifecycle(failures: Int) =
    ZLayer((Ref.make(Chunk.empty[StatusChange]) <*> Ref.make(failures)).map(StubLifecycle(_, _)))

  private def change = StatusChange(java.util.UUID.randomUUID(), EventStatus.Resolved, java.time.Instant.EPOCH)

  private def record(value: String) = RawRecord("raw", 1, 42L, Some("k1"), value)

  private val noDelayRetry = Schedule.recurs(3)

  private def layers(failures: Int) =
    ZLayer.make[RecordingProducer & StubHandler & StubLifecycle & RecordHandler](
      RecordingProducer.layer,
      stub(failures),
      lifecycle(failures),
      ZLayer.succeed(TestKafka.config()),
      TestTelemetry.tracing,
      RecordHandler.withRetry(noDelayRetry),
    )

  def spec = suite("RecordHandler")(
    test("parks a malformed record on the DLQ with its source coordinates") {
      for
        handler  <- ZIO.service[RecordHandler]
        producer <- ZIO.service[RecordingProducer]
        _        <- handler.handle(record("{not json"))
        dlq      <- producer.allRaw
        events   <- producer.all
      yield assertTrue(
        dlq.map(_.topic) == Chunk("dlq"),
        dlq.head.value == "{not json",
        dlq.head.key.contains("k1"),
        dlq.head.headers(RecordHandler.Headers.SourceTopic) == "raw",
        dlq.head.headers(RecordHandler.Headers.SourcePartition) == "1",
        dlq.head.headers(RecordHandler.Headers.SourceOffset) == "42",
        dlq.head.headers(RecordHandler.Headers.Error).nonEmpty,
        dlq.head.headers.contains(RecordHandler.Headers.FailedAt),
        events.isEmpty,
      )
    }.provide(layers(failures = 0)),
    test("hands a valid record to the event handler and leaves the DLQ empty") {
      for
        handler  <- ZIO.service[RecordHandler]
        stub     <- ZIO.service[StubHandler]
        producer <- ZIO.service[RecordingProducer]
        _        <- handler.handle(record(EventCodec.encode(Samples.event())))
        calls    <- stub.callCount
        dlq      <- producer.allRaw
      yield assertTrue(calls == 1, dlq.isEmpty)
    }.provide(layers(failures = 0)),
    test("retries transient failures until the event is processed") {
      for
        handler <- ZIO.service[RecordHandler]
        stub    <- ZIO.service[StubHandler]
        result  <- handler.handle(record(EventCodec.encode(Samples.event()))).exit
        calls   <- stub.callCount
      yield assertTrue(result.isSuccess, calls == 3)
    }.provide(layers(failures = 2)),
    test("fails (so the offset is not committed) when failures persist past the retries") {
      for
        handler <- ZIO.service[RecordHandler]
        result  <- handler.handle(record(EventCodec.encode(Samples.event()))).exit
      yield assertTrue(result.isFailure)
    }.provide(layers(failures = 10)),
    test("hands a record from the status topic to the lifecycle service, not the event handler") {
      val request = change
      for
        handler <- ZIO.service[RecordHandler]
        events  <- ZIO.service[StubHandler]
        stub    <- ZIO.service[StubLifecycle]
        _       <- handler.handle(RawRecord("status", 0, 7L, Some(request.id.toString), EventCodec.encodeChange(request)))
        seen    <- stub.seen
        calls   <- events.callCount
      yield assertTrue(seen == Chunk(request), calls == 0)
    }.provide(layers(failures = 0)),
    test("retries a status change that fails for a moment") {
      val request = change
      for
        handler <- ZIO.service[RecordHandler]
        stub    <- ZIO.service[StubLifecycle]
        result  <- handler.handle(RawRecord("status", 0, 7L, None, EventCodec.encodeChange(request))).exit
        seen    <- stub.seen
      yield assertTrue(result.isSuccess, seen == Chunk(request))
    }.provide(layers(failures = 2)),
    test("parks a malformed status change on the DLQ") {
      for
        handler  <- ZIO.service[RecordHandler]
        producer <- ZIO.service[RecordingProducer]
        stub     <- ZIO.service[StubLifecycle]
        _        <- handler.handle(RawRecord("status", 0, 7L, None, "{\"id\":\"nope\"}"))
        dlq      <- producer.allRaw
        seen     <- stub.seen
      yield assertTrue(
        dlq.map(_.topic) == Chunk("dlq"),
        dlq.head.headers(RecordHandler.Headers.SourceTopic) == "status",
        seen.isEmpty,
      )
    }.provide(layers(failures = 0)),
    test("does not drop a malformed record when the DLQ is briefly unavailable") {
      for
        handler  <- ZIO.service[RecordHandler]
        producer <- ZIO.service[RecordingProducer]
        _        <- producer.failNext(2)
        _        <- handler.handle(record("{not json"))
        dlq      <- producer.allRaw
      yield assertTrue(dlq.size == 1)
    }.provide(layers(failures = 0)),
  )
