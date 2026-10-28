package com.github.olehbohatyi.mep.telemetry

import zio.*
import zio.telemetry.opentelemetry.OpenTelemetry
import zio.telemetry.opentelemetry.context.ContextStorage
import zio.telemetry.opentelemetry.tracing.Tracing

import java.util.logging.{ Level, Logger }

import io.opentelemetry.api
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import io.opentelemetry.sdk.metrics.{ Aggregation, InstrumentSelector, SdkMeterProviderBuilder, View }
import scala.jdk.CollectionConverters.*
import com.github.olehbohatyi.mep.AppLogging

// Traces, metrics and logs over OTLP (grafana/otel-lgtm in compose)
object Telemetry:

  /** What an app's bootstrap provides to its run: the SDK and the one fiber-local context store. Tracing and the
    * logging bridge must share that store, or logs would not carry the active span's ids — and the logger can only be
    * installed at bootstrap. Context lives in a FiberRef, not the agent's thread-local: ZIO moves fibers between
    * threads, and a spike with the Java agent lost the caller's trace at the HTTP boundary.
    */
  type Env = api.OpenTelemetry & ContextStorage

  private val ScopeName = "com.github.olehbohatyi.mep"

  // Console logging as before, plus ZIO logs to OTLP with the trace and span ids of the fiber that wrote them
  def bootstrap(serviceName: String): ZLayer[Any, Throwable, Env] =
    AppLogging.live ++ ((sdk(serviceName) ++ OpenTelemetry.contextZIO) >+> OpenTelemetry.logging(ScopeName))

  val tracing: URLayer[Env, Tracing] = OpenTelemetry.tracing(ScopeName)

  // ZIO metrics (and ZIO's runtime metrics) exported through OpenTelemetry
  val metrics: URLayer[Env, Unit] = OpenTelemetry.metrics(ScopeName) >>> OpenTelemetry.zioMetrics

  // For tests: nothing is exported, but spans still propagate through the fiber context
  val noop: ULayer[Env] = OpenTelemetry.noop ++ OpenTelemetry.contextZIO

  /** Configured by the standard OTEL_* environment variables (OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_SDK_DISABLED, …), which
    * keeps the OpenTelemetry conventions instead of mirroring them in our HOCON; `serviceName` is only the default for
    * OTEL_SERVICE_NAME. Closing the layer flushes what is still buffered.
    */
  private def sdk(serviceName: String): TaskLayer[api.OpenTelemetry] =
    OpenTelemetry.custom(
      ZIO.succeed(histogramLoggers.foreach(_.setLevel(Level.SEVERE))) *> ZIO.fromAutoCloseable(
        ZIO.attempt(
          AutoConfiguredOpenTelemetrySdk
            .builder()
            .addPropertiesSupplier(() => Map("otel.service.name" -> serviceName).asJava)
            .addMeterProviderCustomizer((meters, _) => dropNegativeHistograms(meters))
            .build()
            .getOpenTelemetrySdk
        )
      )
    )

  /** zio-kafka's built-in metrics come along with the bridge, and are worth having (commit latency, partitions, queues)
    * — except these histograms, which zio-kafka feeds negative values. The SDK rejects each negative value with a
    * warning, so the series would be silently incomplete; they are dropped instead.
    */
  private val NegativeHistograms = List(
    "ziokafka_producer_send_queue_size",
    "ziokafka_producer_send_queue_latency_seconds",
    "ziokafka_consumer_command_queue_size",
  )

  /** The SDK rejects each negative value with a warning (java.util.logging, throttled), before views apply, so the drop
    * does not silence it. Only the histograms above ever record one — ours measure durations — so those warnings are
    * noise. Held here because java.util.logging keeps loggers weakly and would forget the level.
    */
  private val histogramLoggers =
    List("SdkDoubleHistogram", "SdkLongHistogram").map(c => Logger.getLogger(s"io.opentelemetry.sdk.metrics.$c"))

  private def dropNegativeHistograms(meters: SdkMeterProviderBuilder): SdkMeterProviderBuilder =
    NegativeHistograms.foldLeft(meters)((builder, name) =>
      builder.registerView(
        InstrumentSelector.builder().setName(name).build(),
        View.builder().setAggregation(Aggregation.drop()).build(),
      )
    )
