package com.github.olehbohatyi.mep.pipe

import zio.*

import com.github.olehbohatyi.mep.broker.ProducerService
import com.github.olehbohatyi.mep.config.{ KafkaConfig, LifecycleConfig }
import com.github.olehbohatyi.mep.domain.{ EventStatus, EventType, StatusChange }
import com.github.olehbohatyi.mep.stores.EventRepo

enum CloseOutcome:
  case Closed(status: EventStatus)
  // Closed before, by an earlier request or by expiry; it keeps that status
  case AlreadyClosed(status: EventStatus)
  // Not a status an event can be closed with, or no such event stored
  case Ignored(reason: String)

/** Closes events: on a client's request (the status topic) and when they expire. Every event this closes, or finds
  * closed, is published to the closed topic for the live map. Publishing again is harmless (consumers key events by id)
  * and covers a pipe that stopped between closing an event and publishing it: a status change is consumed again, and
  * the sweep publishes an event before closing it.
  */
trait LifecycleService:
  def apply(change: StatusChange): Task[CloseOutcome]
  // Expires the open events past their type's lifetime; returns how many it expired
  def expireDue: Task[Int]
  // expireDue every sweep interval, for as long as the pipe runs; a failed sweep is logged and the next one tries again
  def sweep: UIO[Nothing]

object LifecycleService:

  // Events expired per type and query, so a backlog (a pipe that was off) is worked through in bounded steps
  val Batch = 500

  val live: URLayer[EventRepo & ProducerService & KafkaConfig & LifecycleConfig, LifecycleService] =
    ZLayer.fromFunction(LifecycleServiceLive(_, _, _, _))

  final private class LifecycleServiceLive(
      repo: EventRepo,
      producer: ProducerService,
      kafka: KafkaConfig,
      lifecycle: LifecycleConfig,
    ) extends LifecycleService:

    def apply(change: StatusChange): Task[CloseOutcome] =
      if !change.status.isClosed then ZIO.succeed(CloseOutcome.Ignored(s"cannot change an event to ${change.status}"))
      else
        for
          closed  <- repo.close(change.id, change.status, change.requestedAt)
          current <- repo.findById(change.id)
          outcome <- current match
                       case None                                 => ZIO.succeed(CloseOutcome.Ignored("no such event stored"))
                       case Some(event) if event.status.isClosed =>
                         producer
                           .publish(kafka.topics.closed, event)
                           .as(if closed then CloseOutcome.Closed(event.status)
                           else CloseOutcome.AlreadyClosed(event.status))
                       // Still open right after closing it: should not happen; failing gets the request retried
                       case Some(event)                          =>
                         ZIO.fail(IllegalStateException(s"event ${event.id} still ${event.status} after closing it"))
        yield outcome

    def expireDue: Task[Int] =
      Clock.instant.flatMap { now =>
        ZIO
          .foreach(EventType.values.toList) { eventType =>
            val cutoff                      = now.minus(lifecycle.ttl(eventType))
            // Until a batch comes back short: the backlog is done
            def loop(total: Int): Task[Int] =
              repo.findExpired(eventType, cutoff, Batch).flatMap { due =>
                ZIO
                  .foldLeft(due)(0) { (n, event) =>
                    // Published before it is closed: a pipe that stops in between leaves the event open, so the next
                    // sweep finds it again; closed first, no sweep would look at it again
                    producer.publish(
                      kafka.topics.closed,
                      event.copy(status = EventStatus.Expired, closedAt = Some(now)),
                    )
                      *> repo.close(event.id, EventStatus.Expired, now).flatMap {
                        case true  => ZIO.succeed(n + 1)
                        // Another pipe's sweep, or a client, closed it first: publish what is stored, after ours
                        case false => republish(event.id).as(n)
                      }
                  }
                  .flatMap(n => if due.size < Batch then ZIO.succeed(total + n) else loop(total + n))
              }
            loop(0)
          }
          .map(_.sum)
      }

    private def republish(id: java.util.UUID): Task[Unit] =
      repo.findById(id).flatMap(ZIO.foreachDiscard(_)(producer.publish(kafka.topics.closed, _)))

    def sweep: UIO[Nothing] =
      expireDue
        .tap(n => PipeMetrics.closed(EventStatus.Expired, n) *> ZIO.logInfo(s"Expired $n event(s)").when(n > 0))
        .catchAllCause(cause => ZIO.logWarningCause("expiry sweep failed; trying again next interval", cause))
        .repeat(Schedule.spaced(lifecycle.sweepInterval))
        *> ZIO.never
