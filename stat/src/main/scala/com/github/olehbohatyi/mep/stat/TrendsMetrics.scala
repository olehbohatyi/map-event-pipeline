package com.github.olehbohatyi.mep.stat

import java.util.concurrent.atomic.AtomicLong

import io.opentelemetry.api.metrics.{ Meter, ObservableLongGauge }
import scala.jdk.CollectionConverters.*

/** What one micro-batch of the trends query did, from its progress. `lag` is known only for a Kafka source,
  * `durationMs` only once the batch ran.
  */
final case class BatchStats(
    inputRows: Long,
    droppedLate: Long,
    skipped: Long,
    lag: Option[Long],
    durationMs: Option[Long],
  )

/** The trends job's metrics. Spark reports most of this only in each batch's progress (and the Spark UI, which the
  * compose stack does not expose), so the listener in TrendsApp records it here. No labels: one query, one topic.
  */
final class TrendsMetrics(meter: Meter):

  private val inputRows = meter
    .counterBuilder("mep_trends_input_records")
    .setDescription("Kafka records the trends query read")
    .build()

  private val lateEvents = meter
    .counterBuilder("mep_trends_late_events")
    .setDescription(s"Events dropped as older than the ${Trends.Watermark} watermark; their hours are undercounted")
    .build()

  private val skippedRecords = meter
    .counterBuilder("mep_trends_skipped_records")
    .setDescription("Records the topic's retention deleted before the query read them; their hours are undercounted")
    .build()

  private val upsertedRows = meter
    .counterBuilder("mep_trends_upserted_rows")
    .setDescription("Hourly counts written to map_event_trends")
    .build()

  private val queryFailures = meter
    .counterBuilder("mep_trends_query_failures")
    .setDescription("Times the trends query stopped with an error")
    .build()

  private val batchDuration = meter
    .histogramBuilder("mep_trends_batch_duration_ms")
    .setDescription("Time a micro-batch took, from trigger to commit")
    .ofLongs()
    .setExplicitBucketBoundariesAdvice(
      List(50L, 100L, 250L, 500L, 1000L, 2500L, 5000L, 10000L, 30000L, 60000L, 120000L).map(Long.box).asJava
    )
    .build()

  // Last known value; none is reported before the first Kafka batch
  private val lastLag = AtomicLong(-1L)

  private val lagGauge: ObservableLongGauge = meter
    .gaugeBuilder("mep_trends_kafka_lag")
    .setDescription("Records on the topic that the query had not read yet, as of its last batch")
    .ofLongs()
    .buildWithCallback { m =>
      val lag = lastLag.get()
      if lag >= 0 then m.record(lag)
    }

  def batch(stats: BatchStats): Unit =
    inputRows.add(stats.inputRows)
    lateEvents.add(stats.droppedLate)
    skippedRecords.add(stats.skipped)
    stats.lag.foreach(lastLag.set)
    stats.durationMs.foreach(ms => batchDuration.record(ms))

  def upserted(rows: Int): Unit = upsertedRows.add(rows.toLong)

  def failed(): Unit = queryFailures.add(1L)

  def close(): Unit = lagGauge.close()
