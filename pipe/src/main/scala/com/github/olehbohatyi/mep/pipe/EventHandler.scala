package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.telemetry.opentelemetry.tracing.Tracing

import com.github.olehbohatyi.mep.broker.ProducerService
import com.github.olehbohatyi.mep.config.KafkaConfig
import com.github.olehbohatyi.mep.domain.MapEvent
import com.github.olehbohatyi.mep.stores.EventRepo

enum Outcome:
  case Processed(topic: String)
  case Duplicate

// Processes one raw event: dedup → enrich → escalate → persist → publish.
// Safe to re-run for the same event (at-least-once delivery from Kafka).
trait EventHandler:
  def handle(event: MapEvent): Task[Outcome]

object EventHandler:

  val live: URLayer[
    KafkaConfig & EventRepo & ProducerService & DeduplicationService & EnrichmentService & EscalationService & Tracing,
    EventHandler,
  ] =
    ZLayer.fromFunction(EventHandlerLive(_, _, _, _, _, _, _))

  final private class EventHandlerLive(
      kafka: KafkaConfig,
      repo: EventRepo,
      producer: ProducerService,
      dedup: DeduplicationService,
      enrichment: EnrichmentService,
      escalation: EscalationService,
      tracing: Tracing,
    ) extends EventHandler:

    def handle(event: MapEvent): Task[Outcome] =
      tracing.span("dedup check")(dedup.isProcessed(event)).flatMap {
        case true  => ZIO.succeed(Outcome.Duplicate)
        case false =>
          for
            enriched  <- tracing.span("enrich")(enrichment.enrich(event))
            escalated <- tracing.span("escalate")(escalation.escalate(enriched))
            inserted  <- tracing.span("store")(repo.insert(escalated))
            // Stored before and closed since (a redelivery, or a dedup key that expired): publishing it as enriched
            // would put a closed event back on the live map
            closed    <- if inserted then ZIO.succeed(false) else repo.findById(event.id).map(_.exists(_.status.isClosed))
            outcome   <- if closed then ZIO.succeed(Outcome.Duplicate) else publish(escalated)
            _         <- tracing.span("dedup mark")(dedup.markProcessed(event))
          yield outcome
      }

    // Publish even if the row already existed: a previous attempt may have
    // crashed between insert and publish. Consumers dedup by event id.
    private def publish(event: MapEvent): Task[Outcome] =
      val topic = escalation.topicFor(event)
      producer.publish(kafka.topics.enriched, event) *> producer.publish(topic, event).as(Outcome.Processed(topic))
