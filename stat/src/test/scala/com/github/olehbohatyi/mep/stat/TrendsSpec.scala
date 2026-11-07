package com.github.olehbohatyi.mep.stat

import zio.*
import zio.test.*

import java.nio.file.{ Files, Path }
import java.time.Instant

import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.*
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.*

// Runs the streaming query over text files (one MapEvent JSON per line) with a sink that keeps what an upserting
// table would hold: the last count written per (hour, region, type). Kafka and Postgres are left to the stack.
object TrendsSpec extends ZIOSpecDefault:

  private type Key = (Instant, String, String)

  // map_events' columns that the recount reads
  private val rowSchema = StructType(
    Seq(
      StructField("event_type", StringType),
      StructField("region", StringType),
      StructField("status", StringType),
      StructField("created_at", TimestampType),
    )
  )

  private def event(id: String, createdAt: String, eventType: String = "Hazard", region: String = "Kyiv City") =
    s"""{"id":"$id","eventType":"$eventType","severity":"Low","status":"Enriched","location":{"lat":50.45,"lon":30.52},""" +
      s""""region":"$region","occurredAt":"$createdAt","createdAt":"$createdAt"}"""

  private def hour(h: Int) = Instant.parse(f"2026-10-27T$h%02d:00:00Z")

  // A directory the stream reads, its checkpoint, and the table the sink fills
  final private case class Stream(in: Path, checkpoint: Path, table: TrieMap[Key, Long]):

    def add(name: String, lines: String*): Task[Unit] =
      ZIO.attemptBlocking(Files.writeString(in.resolve(s"$name.json"), lines.mkString("\n"))).unit

    // Processes what is there and stops, like a restart of the job would from the same checkpoint.
    // Returns how many rows the run dropped as late.
    def runOnce: RIO[SparkSession, Long] =
      ZIO.serviceWithZIO[SparkSession] { spark =>
        ZIO.attemptBlocking {
          val query = Trends
            .start(spark.readStream.text(in.toString), checkpoint.toString, Trigger.AvailableNow()) { (batch, _) =>
              collect(batch).foreach(table.put)
            }
          query.awaitTermination()
          query.recentProgress.map(Trends.droppedLate).sum
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
        s       <- stream
        _       <- s.add("1", event("a", "2026-10-27T10:05:00Z"), event("b", "2026-10-27T11:20:00Z"))
        early   <- s.runOnce
        first    = s.counts
        // The watermark is now 11:10: 10:05 is late, 11:15 and 11:40 are not
        _       <- s.add(
                     "2",
                     event("late", "2026-10-27T10:05:00Z"),
                     event("a", "2026-10-27T10:05:00Z"),
                     event("c", "2026-10-27T11:15:00Z"),
                     event("d", "2026-10-27T11:40:00Z"),
                     event("d", "2026-10-27T11:40:00Z"),
                   )
        dropped <- s.runOnce
      yield assertTrue(
        first == Map((hour(10), "Kyiv City", "Hazard") -> 1L, (hour(11), "Kyiv City", "Hazard") -> 1L),
        s.counts == Map((hour(10), "Kyiv City", "Hazard") -> 1L, (hour(11), "Kyiv City", "Hazard") -> 3L),
        // Regression: late events were dropped without a trace; the job now logs this count per batch
        early == 0L,
        dropped == 2L,
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
    // The recount must cut hours exactly like the stream, or it would overwrite a correct count with another one
    test("a recount from map_events writes the same counts as the stream") {
      val created = List(
        ("a", "2026-10-27T10:05:00.123456Z", "Kyiv City", "Hazard"),
        ("b", "2026-10-27T10:59:59.999999Z", "Kyiv City", "Hazard"),
        ("c", "2026-10-27T11:00:00Z", "Kyiv City", "Hazard"),
        ("d", "2026-10-27T11:30:00Z", "Lviv Oblast", "Sign"),
      )
      for
        s       <- stream
        _       <- s.add("1", created.map((id, at, region, t) => event(id, at, eventType = t, region = region))*)
        _       <- s.runOnce
        spark   <- ZIO.service[SparkSession]
        rows     = created.map((_, at, region, t) => Row(t, region, "Enriched", java.sql.Timestamp.from(Instant.parse(at))))
        recount <- ZIO.attemptBlocking(
                     collect(
                       TrendsBackfill
                         .recount(spark.createDataFrame(rows.asJava, rowSchema), TrendsBackfill.Range(None, hour(12)))
                     ).toMap
                   )
      yield assertTrue(s.counts.size == 3, recount == s.counts)
    },
    // Regression: records deleted by retention while the job was stopped were skipped with only Spark's warning
    test("counts the records a batch skipped because retention deleted them") {
      val topic = "map.events.enriched"
      assertTrue(
        Trends.skippedRecords(s"""{"$topic":{"0":30,"1":30}}""", s"""{"$topic":{"0":7893,"1":7868}}""", 11L) ==
          (7863L + 7838L - 11L),
        Trends.skippedRecords(s"""{"$topic":{"0":30}}""", s"""{"$topic":{"0":40}}""", 10L) == 0L,
        // A new partition is read from its start, a fresh checkpoint has no start offsets
        Trends.skippedRecords(s"""{"$topic":{"0":30}}""", s"""{"$topic":{"0":40,"1":5}}""", 15L) == 0L,
        Trends.skippedRecords(null, s"""{"$topic":{"0":7890}}""", 3L) == 0L,
      )
    },
  ).provideLayerShared(TestSpark.layer) @@ TestAspect.sequential @@ TestAspect.withLiveClock @@ TestAspect.timeout(
    5.minutes
  )
