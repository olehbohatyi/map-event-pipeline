package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.metrics.{ Metric, MetricKeyType, MetricLabel }

// ZIO metrics, exported over OTLP by Telemetry.metrics (Prometheus adds `_total` to counters)
object PipeMetrics:

  private val processed    = Metric.counter("mep_events_processed")
  private val deadLettered = Metric.counter("mep_records_dead_lettered")
  private val duration     =
    Metric.histogram("mep_event_processing_duration_ms", MetricKeyType.Histogram.Boundaries.exponential(1, 2, 14))

  // `outcome` is the target topic, or "duplicate"
  def handled(outcome: String, took: Duration): UIO[Unit] =
    processed.tagged(MetricLabel("outcome", outcome)).increment *> duration.update(took.toNanos / 1e6)

  val deadLetter: UIO[Unit] = deadLettered.increment
