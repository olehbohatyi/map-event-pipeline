package com.github.olehbohatyi.mep.stat

import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import scala.jdk.CollectionConverters.*

/** Metrics over OTLP for the Spark jobs. They do not run on ZIO and stat does not depend on core, so this is the plain
  * OpenTelemetry SDK rather than core's Telemetry, configured by the same standard OTEL_* variables
  * (OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_SDK_DISABLED, …); `serviceName` is only the default for OTEL_SERVICE_NAME. Only
  * metrics are exported: Spark logs through its own log4j setup, and a micro-batch is not a request worth a trace. The
  * SDK registers a JVM shutdown hook that flushes what is still buffered.
  */
object StatTelemetry:

  val ScopeName = "com.github.olehbohatyi.mep.stat"

  def meter(serviceName: String): Meter =
    sdk(serviceName).getMeter(ScopeName)

  private def sdk(serviceName: String): OpenTelemetrySdk =
    AutoConfiguredOpenTelemetrySdk
      .builder()
      .addPropertiesSupplier(() =>
        Map(
          "otel.service.name"    -> serviceName,
          "otel.traces.exporter" -> "none",
          "otel.logs.exporter"   -> "none",
        ).asJava
      )
      .build()
      .getOpenTelemetrySdk
