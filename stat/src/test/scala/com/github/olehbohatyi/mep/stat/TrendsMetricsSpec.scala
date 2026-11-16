package com.github.olehbohatyi.mep.stat

import zio.*
import zio.test.*

import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.MetricData
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import scala.jdk.CollectionConverters.*

object TrendsMetricsSpec extends ZIOSpecDefault:

  final private case class Recorded(reader: InMemoryMetricReader):
    private def metrics: Map[String, MetricData] =
      reader.collectAllMetrics().asScala.map(m => m.getName -> m).toMap

    def sum(name: String): Option[Long]            =
      metrics.get(name).map(_.getLongSumData.getPoints.asScala.map(_.getValue).sum)
    def gauge(name: String): Option[Long]          =
      metrics.get(name).flatMap(_.getLongGaugeData.getPoints.asScala.headOption.map(_.getValue))
    def histogramCount(name: String): Option[Long] =
      metrics.get(name).map(_.getHistogramData.getPoints.asScala.map(_.getCount).sum)

  private def recording: UIO[(TrendsMetrics, Recorded)] =
    ZIO.succeed {
      val reader = InMemoryMetricReader.create()
      val meter  = SdkMeterProvider.builder().registerMetricReader(reader).build().get(StatTelemetry.ScopeName)
      (TrendsMetrics(meter), Recorded(reader))
    }

  def spec = suite("TrendsMetrics")(
    // Regression: late and skipped records only went into log lines, which nothing collects from the Spark container
    test("adds up each batch's records, late events and skipped records; the lag is the last batch's") {
      for
        (metrics, recorded) <- recording
        _                   <- ZIO.succeed {
                                 metrics.batch(BatchStats(10L, 2L, 0L, Some(40L), Some(120L)))
                                 metrics.batch(BatchStats(5L, 0L, 3L, Some(7L), Some(80L)))
                                 // A batch from a source without offsets keeps the last known lag
                                 metrics.batch(BatchStats(1L, 0L, 0L, None, None))
                                 metrics.upserted(4)
                               }
      yield assertTrue(
        recorded.sum("mep_trends_input_records").contains(16L),
        recorded.sum("mep_trends_late_events").contains(2L),
        recorded.sum("mep_trends_skipped_records").contains(3L),
        recorded.sum("mep_trends_upserted_rows").contains(4L),
        recorded.gauge("mep_trends_kafka_lag").contains(7L),
        recorded.histogramCount("mep_trends_batch_duration_ms").contains(2L),
      )
    },
    test("reports no lag before the first Kafka batch, and counts query failures") {
      for
        (metrics, recorded) <- recording
        _                   <- ZIO.succeed(metrics.failed())
      yield assertTrue(
        recorded.gauge("mep_trends_kafka_lag").isEmpty,
        recorded.sum("mep_trends_query_failures").contains(1L),
      )
    },
  )
