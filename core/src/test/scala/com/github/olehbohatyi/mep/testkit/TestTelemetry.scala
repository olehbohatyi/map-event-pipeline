package com.github.olehbohatyi.mep.testkit

import zio.*
import zio.telemetry.opentelemetry.OpenTelemetry
import zio.telemetry.opentelemetry.tracing.Tracing

import io.opentelemetry.api
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import io.opentelemetry.sdk.trace.SdkTracerProvider
import com.github.olehbohatyi.mep.telemetry.Telemetry

object TestTelemetry:

  // Spans propagate through the fiber context but are exported nowhere
  val tracing: ULayer[Tracing] = Telemetry.noop >>> Telemetry.tracing

  // Every ended span lands in the exporter, so tests can check parents, kinds and trace ids
  val recording: ULayer[Tracing & InMemorySpanExporter] =
    val exporter = ZLayer.succeed(InMemorySpanExporter.create())
    val sdk      = exporter >>> ZLayer.fromFunction((spans: InMemorySpanExporter) =>
      OpenTelemetrySdk
        .builder()
        .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spans)).build())
        .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
        .build(): api.OpenTelemetry
    )
    exporter ++ ((sdk ++ OpenTelemetry.contextZIO) >>> Telemetry.tracing)
