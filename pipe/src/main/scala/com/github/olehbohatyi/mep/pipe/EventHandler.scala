package com.github.olehbohatyi.mep.pipe

import zio.*

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
    KafkaConfig & EventRepo & ProducerService & DeduplicationService & EnrichmentService & EscalationService,
    EventHandler,
  ] =
    ZLayer.fromFunction(EventHandlerLive(_, _, _, _, _, _))

  final private class EventHandlerLive(
      kafka: KafkaConfig,
      repo: EventRepo,
      producer: ProducerService,
      dedup: DeduplicationService,
      enrichment: EnrichmentService,
      escalation: EscalationService,
    ) extends EventHandler:

    def handle(event: MapEvent): Task[Outcome] =
      dedup.isProcessed(event).flatMap {
        case true  => ZIO.succeed(Outcome.Duplicate)
        case false =>
          for
            enriched  <- enrichment.enrich(event)
            escalated <- escalation.escalate(enriched)
            _         <- repo.insert(escalated)
            topic      = escalation.topicFor(escalated)
            // Publish even if the row already existed: a previous attempt may have
            // crashed between insert and publish. Consumers dedup by event id.
            _         <- producer.publish(kafka.topics.enriched, escalated)
            _         <- producer.publish(topic, escalated)
            _         <- dedup.markProcessed(event)
          yield Outcome.Processed(topic)
      }
