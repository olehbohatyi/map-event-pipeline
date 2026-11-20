package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.metrics.{ Metric, MetricKeyType, MetricLabel }

import com.github.olehbohatyi.mep.domain.EventStatus

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

  private val closedEvents = Metric.counter("mep_events_closed")

  // Events this pipe closed, by the status they were closed with (Resolved or Expired)
  def closed(status: EventStatus, n: Int): UIO[Unit] =
    closedEvents.tagged(MetricLabel("status", status.toString)).incrementBy(n.toLong).when(n > 0).unit
