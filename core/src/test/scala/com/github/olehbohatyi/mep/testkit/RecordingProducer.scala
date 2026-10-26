package com.github.olehbohatyi.mep.testkit

import zio.*

import com.github.olehbohatyi.mep.broker.ProducerService
import com.github.olehbohatyi.mep.domain.MapEvent

final case class RawPublish(topic: String, key: Option[String], value: String, headers: Map[String, String])

// Records published events and raw payloads; fails the next `failures` publishes of either kind
final class RecordingProducer(
    published: Ref[Chunk[(String, MapEvent)]],
    raw: Ref[Chunk[RawPublish]],
    failures: Ref[Int],
  ) extends ProducerService:

  private def failOr(record: UIO[Unit]): Task[Unit] =
    failures.getAndUpdate(n => math.max(0, n - 1)).flatMap { n =>
      if n > 0 then ZIO.fail(RuntimeException("broker unavailable")) else record
    }

  def publish(topic: String, event: MapEvent): Task[Unit] =
    failOr(published.update(_ :+ (topic -> event)))

  def publishRaw(topic: String, key: Option[String], value: String, headers: Map[String, String]): Task[Unit] =
    failOr(raw.update(_ :+ RawPublish(topic, key, value, headers)))

  def failNext(n: Int): UIO[Unit]         = failures.set(n)
  def all: UIO[Chunk[(String, MapEvent)]] = published.get
  def allRaw: UIO[Chunk[RawPublish]]      = raw.get

object RecordingProducer:
  val layer: ULayer[RecordingProducer] =
    ZLayer(
      (Ref.make(Chunk.empty[(String, MapEvent)]) <*> Ref.make(Chunk.empty[RawPublish]) <*> Ref.make(0))
        .map(RecordingProducer(_, _, _))
    )
