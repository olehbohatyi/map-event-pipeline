package com.github.olehbohatyi.mep.stat

import zio.*
import zio.test.*

import java.nio.file.{ Files, Path }
import java.time.Instant

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.streaming.Trigger
import scala.collection.concurrent.TrieMap

// Runs the streaming query over text files (one MapEvent JSON per line) with a sink that keeps what an upserting
// table would hold: the last count written per (hour, region, type). Kafka and Postgres are left to the stack.
object TrendsSpec extends ZIOSpecDefault:

  private type Key = (Instant, String, String)

  private def event(id: String, createdAt: String, eventType: String = "Hazard", region: String = "Kyiv City") =
    s"""{"id":"$id","eventType":"$eventType","severity":"Low","status":"Enriched","location":{"lat":50.45,"lon":30.52},""" +
      s""""region":"$region","occurredAt":"$createdAt","createdAt":"$createdAt"}"""

  private def hour(h: Int) = Instant.parse(f"2026-10-27T$h%02d:00:00Z")

  // A directory the stream reads, its checkpoint, and the table the sink fills
  final private case class Stream(in: Path, checkpoint: Path, table: TrieMap[Key, Long]):

    def add(name: String, lines: String*): Task[Unit] =
      ZIO.attemptBlocking(Files.writeString(in.resolve(s"$name.json"), lines.mkString("\n"))).unit

    // Processes what is there and stops, like a restart of the job would from the same checkpoint
    def runOnce: RIO[SparkSession, Unit] =
      ZIO.serviceWithZIO[SparkSession] { spark =>
        ZIO.attemptBlocking {
          Trends
            .start(spark.readStream.text(in.toString), checkpoint.toString, Trigger.AvailableNow()) { (batch, _) =>
              collect(batch).foreach(table.put)
            }
            .awaitTermination()
        }
      }

    def counts: Map[Key, Long] = table.toMap

  private def collect(batch: DataFrame): Array[(Key, Long)] =
    batch
      .collect()
      .map(r => (r.getTimestamp(0).toInstant, r.getString(1), r.getString(2)) -> r.getLong(3))

  private val stream: Task[Stream] =
    ZIO.attemptBlocking(
      Stream(Files.createTempDirectory("trends-in"), Files.createTempDirectory("trends-ckpt"), TrieMap.empty)
    )

  def spec = suite("Trends")(
    test("counts events per hour, region and type; drops duplicates, unreadable records and unresolved regions") {
      for
        s <- stream
        _ <- s.add(
               "1",
               // nanosecond precision, as the pipe writes java.time.Instant
               event("a", "2026-10-27T10:05:00.123456789Z"),
               event("a", "2026-10-27T10:05:00.123456789Z"),
               event("b", "2026-10-27T10:40:00Z"),
               event("c", "2026-10-27T10:50:00Z", eventType = "Sign"),
               event("d", "2026-10-27T10:55:00Z", region = "Lviv Oblast"),
               event("e", "2026-10-27T11:02:00Z"),
               "{oops",
               event("n", "2026-10-27T10:30:00Z").replace(""""region":"Kyiv City",""", ""),
             )
        _ <- s.runOnce
      yield assertTrue(
        s.counts == Map(
          (hour(10), "Kyiv City", "Hazard")   -> 2L,
          (hour(10), "Kyiv City", "Sign")     -> 1L,
          (hour(10), "Lviv Oblast", "Hazard") -> 1L,
          (hour(11), "Kyiv City", "Hazard")   -> 1L,
        )
      )
    },
    test("after a restart from the checkpoint it updates the counts and drops what is now late") {
      for
        s    <- stream
        _    <- s.add("1", event("a", "2026-10-27T10:05:00Z"), event("b", "2026-10-27T11:20:00Z"))
        _    <- s.runOnce
        first = s.counts
        // The watermark is now 11:10: 10:05 is late, 11:15 and 11:40 are not
        _    <- s.add(
                  "2",
                  event("late", "2026-10-27T10:05:00Z"),
                  event("a", "2026-10-27T10:05:00Z"),
                  event("c", "2026-10-27T11:15:00Z"),
                  event("d", "2026-10-27T11:40:00Z"),
                  event("d", "2026-10-27T11:40:00Z"),
                )
        _    <- s.runOnce
      yield assertTrue(
        first == Map((hour(10), "Kyiv City", "Hazard") -> 1L, (hour(11), "Kyiv City", "Hazard") -> 1L),
        s.counts == Map((hour(10), "Kyiv City", "Hazard") -> 1L, (hour(11), "Kyiv City", "Hazard") -> 3L),
      )
    },
    test("windows by the gate's receive time, so a client's wrong clock cannot make later events late") {
      val wrongClock =
        event("future", "2026-10-27T10:00:00Z")
          .replace(""""occurredAt":"2026-10-27T10:00:00Z"""", """"occurredAt":"2031-01-01T00:00:00Z"""")
      for
        s <- stream
        _ <- s.add("1", wrongClock)
        _ <- s.runOnce
        _ <- s.add("2", event("next", "2026-10-27T10:20:00Z"))
        _ <- s.runOnce
      yield assertTrue(s.counts == Map((hour(10), "Kyiv City", "Hazard") -> 2L))
    },
  ).provideLayerShared(TestSpark.layer) @@ TestAspect.sequential @@ TestAspect.withLiveClock @@ TestAspect.timeout(
    5.minutes
  )
