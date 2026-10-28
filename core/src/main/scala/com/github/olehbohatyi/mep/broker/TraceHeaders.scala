package com.github.olehbohatyi.mep.broker

import zio.*
import zio.telemetry.opentelemetry.context.{ IncomingContextCarrier, OutgoingContextCarrier }
import zio.telemetry.opentelemetry.tracing.Tracing
import zio.telemetry.opentelemetry.tracing.propagation.TraceContextPropagator

import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.SpanKind
import scala.collection.mutable

// W3C trace context (`traceparent`, `tracestate`) in Kafka record headers, so a trace continues from the gate's HTTP
// request through the topics to the pipe
object TraceHeaders:

  // The current span's context, to add to an outgoing record
  def ofCurrentSpan(tracing: Tracing): UIO[Map[String, String]] =
    val carrier = OutgoingContextCarrier.default()
    tracing.injectSpan(TraceContextPropagator.default, carrier).as(carrier.kernel.toMap)

  // Runs `zio` in a consumer span that continues the trace the record's headers carry (a new trace when they carry none)
  def continue[R, E, A](
      tracing: Tracing,
      headers: Map[String, String],
      spanName: String,
      attributes: Attributes,
    )(
      zio: => ZIO[R, E, A]
    ): ZIO[R, E, A] =
    tracing.extractSpan(
      TraceContextPropagator.default,
      IncomingContextCarrier.default(mutable.Map.from(headers)),
      spanName,
      SpanKind.CONSUMER,
      attributes,
    )(zio)
