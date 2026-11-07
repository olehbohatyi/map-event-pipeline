package com.github.olehbohatyi.mep.gate

import zio.*
import zio.metrics.{ Metric, MetricKeyType, MetricLabel }

// ZIO metrics, exported over OTLP by Telemetry.metrics (Prometheus adds `_total` to counters)
object GateMetrics:

  private val requests = Metric.counter("mep_http_server_requests")
  private val duration =
    Metric.histogram("mep_http_server_duration_ms", MetricKeyType.Histogram.Boundaries.exponential(1, 2, 14))
  private val ingested = Metric.counter("mep_events_ingested")
  private val limits   = Metric.counter("mep_rate_limit_decisions")

  // Connected live map clients
  val liveSubscribers: Metric.Gauge[Double] = Metric.gauge("mep_live_subscribers")

  // `route` must stay a small set of values: a route's template, never the path a client sent, and `unmatched` for
  // paths no route matches
  def request(method: String, route: String, status: Int, took: Duration): UIO[Unit] =
    val labels = Set(MetricLabel("method", method), MetricLabel("route", route), MetricLabel("status", status.toString))
    requests.tagged(labels).increment *> duration.tagged(labels).update(took.toNanos / 1e6)

  def ingest(request: EventRequest, result: String): UIO[Unit] =
    ingested
      .tagged(
        MetricLabel("event_type", request.eventType.toString),
        MetricLabel("severity", request.severity.toString),
        MetricLabel("result", result),
      )
      .increment

  // `result`: allowed, limited, or unavailable (Redis failed and the request was let through)
  def rateLimit(result: String): UIO[Unit] =
    limits.tagged(MetricLabel("result", result)).increment
