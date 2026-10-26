package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.test.*

import com.github.olehbohatyi.mep.broker.EventCodec
import com.github.olehbohatyi.mep.domain.MapEvent
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

  private def record(value: String) = RawRecord("raw", 1, 42L, Some("k1"), value)

  private val noDelayRetry = Schedule.recurs(3)

  private def layers(failures: Int) =
    ZLayer.make[RecordingProducer & StubHandler & RecordHandler](
      RecordingProducer.layer,
      stub(failures),
      ZLayer.succeed(TestKafka.config()),
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
