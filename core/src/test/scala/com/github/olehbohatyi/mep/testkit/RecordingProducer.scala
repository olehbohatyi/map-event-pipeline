package com.github.olehbohatyi.mep.testkit

import zio.*

import com.github.olehbohatyi.mep.broker.ProducerService
import com.github.olehbohatyi.mep.domain.MapEvent

// Records published events; fails the next `failures` publishes
final class RecordingProducer(published: Ref[Chunk[(String, MapEvent)]], failures: Ref[Int]) extends ProducerService:
  def publish(topic: String, event: MapEvent): Task[Unit] =
    failures.getAndUpdate(n => math.max(0, n - 1)).flatMap { n =>
      if n > 0 then ZIO.fail(RuntimeException("broker unavailable"))
      else published.update(_ :+ (topic -> event))
    }
  def failNext(n: Int): UIO[Unit]                         = failures.set(n)
  def all: UIO[Chunk[(String, MapEvent)]]                 = published.get

object RecordingProducer:
  val layer: ULayer[RecordingProducer] =
    ZLayer((Ref.make(Chunk.empty[(String, MapEvent)]) <*> Ref.make(0)).map(RecordingProducer(_, _)))
