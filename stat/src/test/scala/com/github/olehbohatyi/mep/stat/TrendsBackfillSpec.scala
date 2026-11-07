package com.github.olehbohatyi.mep.stat

import zio.*
import zio.test.*

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.*
import scala.jdk.CollectionConverters.*

object TrendsBackfillSpec extends ZIOSpecDefault:

  private val schema = StructType(
    Seq(
      StructField("event_type", StringType),
      StructField("region", StringType, nullable = true),
      StructField("status", StringType),
      StructField("created_at", TimestampType),
    )
  )

  private def at(time: String) = Instant.parse(s"2026-10-27T${time}Z")
  private def hour(h: Int)     = at(f"$h%02d:00:00")
  private def row(
      time: String,
      eventType: String = "Hazard",
      region: String = "Kyiv City",
      status: String = "Enriched",
    )                          =
    Row(eventType, region, status, Timestamp.from(at(time)))

  private val events = List(
    row("09:59:59"),
    row("10:00:00"),
    row("10:30:00.123456"),
    row("10:45:00", eventType = "Sign"),
    row("10:50:00", region = "Lviv Oblast"),
    row("11:10:00"),
    row("12:05:00"),
    row("10:20:00", status = "Raw"),
    row("10:25:00", region = null),
  )

  private def recount(range: TrendsBackfill.Range): RIO[SparkSession, Map[(Instant, String, String), Long]] =
    ZIO.serviceWithZIO[SparkSession] { spark =>
      ZIO.attemptBlocking(
        TrendsBackfill
          .recount(spark.createDataFrame(events.asJava, schema), range)
          .collect()
          .map(r => (r.getTimestamp(0).toInstant, r.getString(1), r.getString(2)) -> r.getLong(3))
          .toMap
      )
    }

  def spec = suite("TrendsBackfill")(
    // Regression: hours the stream missed (records deleted by retention while the job was stopped) stayed
    // undercounted for good; the recount writes the same rows as the stream, from map_events
    test("recounts enriched events with a region per hour, region and type, within the range") {
      for counts <- recount(TrendsBackfill.Range(Some(hour(10)), hour(12)))
      yield assertTrue(
        counts == Map(
          (hour(10), "Kyiv City", "Hazard")   -> 2L,
          (hour(10), "Kyiv City", "Sign")     -> 1L,
          (hour(10), "Lviv Oblast", "Hazard") -> 1L,
          (hour(11), "Kyiv City", "Hazard")   -> 1L,
        )
      )
    },
    test("without a start it recounts from the first event") {
      for counts <- recount(TrendsBackfill.Range(None, hour(11)))
      yield assertTrue(counts.keySet.map(_._1) == Set(hour(9), hour(10)))
    },
    test("recounts whole hours that ended an hour ago or more") {
      val now = at("12:20:00")
      assertTrue(
        TrendsBackfill.range(None, None, now) == TrendsBackfill.Range(None, hour(11)),
        TrendsBackfill.range(Some(at("09:40:00")), Some(at("10:30:00")), now) ==
          TrendsBackfill.Range(Some(hour(9)), hour(10)),
        TrendsBackfill.range(None, Some(at("18:00:00")), now) == TrendsBackfill.Range(None, hour(11)),
      )
    },
    test("takes --from and --to as instants or UTC dates") {
      assertTrue(
        TrendsBackfill.parseArgs(Nil) == Right((None, None)),
        TrendsBackfill.parseArgs(Seq("--from", "2026-10-27", "--to", "2026-10-27T11:30:00Z")) ==
          Right((Some(hour(0)), Some(at("11:30:00")))),
        TrendsBackfill.parseArgs(Seq("--from", "yesterday")).isLeft,
        TrendsBackfill.parseArgs(Seq("--since", "2026-10-27")).isLeft,
      )
    },
  ).provideLayerShared(TestSpark.layer) @@ TestAspect.withLiveClock
