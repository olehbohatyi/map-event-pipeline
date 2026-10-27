package com.github.olehbohatyi.mep.it

import zio.*
import zio.test.*

import java.util.UUID

import com.github.olehbohatyi.mep.broker.{ ProducerService, TopicBootstrap }
import com.github.olehbohatyi.mep.config.KafkaConfig
import com.github.olehbohatyi.mep.gate.LiveFeed
import com.github.olehbohatyi.mep.testkit.Samples

// The gate's live feed against a real broker: what the pipe publishes to the enriched topic reaches the map
object LiveFeedKafkaSpec extends ZIOSpecDefault:

  // Collects the ids a feed delivers
  private def collect(feed: LiveFeed): URIO[Scope, Ref[Set[UUID]]] =
    for
      seen <- Ref.make(Set.empty[UUID])
      _    <- feed.subscribe(None).runForeach(e => seen.update(_ + e.id)).forkScoped
      _    <- feed.subscribers.repeatUntil(_ == 1)
    yield seen

  def spec = (suite("LiveFeed over Kafka")(
    test("every gate instance sees every enriched event, and an unreadable record is skipped") {
      ZIO.scoped {
        for
          cfg      <- ZIO.service[KafkaConfig]
          producer <- ZIO.service[ProducerService]
          // Two gate instances, each with its own consumer group
          a        <- LiveFeed.live.build.map(_.get[LiveFeed])
          b        <- LiveFeed.live.build.map(_.get[LiveFeed])
          seenA    <- collect(a)
          seenB    <- collect(b)
          // Each starts at the newest offset once it has its partitions, so earlier records are not replayed:
          // publish probes until both have one
          ready     = seenA.get.zipWith(seenB.get)(_.nonEmpty && _.nonEmpty)
          _        <- (producer.publish(cfg.topics.enriched, Samples.event()) *> ZIO.sleep(500.millis) *> ready)
                        .repeatUntil(identity)
                        .timeoutFail(RuntimeException("feeds never received a probe"))(90.seconds)
          target    = Samples.event()
          _        <- producer.publishRaw(cfg.topics.enriched, Some("bad"), "{oops", Map.empty)
          _        <- producer.publish(cfg.topics.enriched, target)
          bothGot   = seenA.get.zipWith(seenB.get)((x, y) => x(target.id) && y(target.id))
          _        <- (ZIO.sleep(250.millis) *> bothGot)
                        .repeatUntil(identity)
                        .timeoutFail(RuntimeException("target event not delivered to both feeds"))(60.seconds)
        yield assertCompletes
      }
    }
  ) @@ TestAspect.beforeAll(TopicBootstrap.run.orDie) @@ TestAspect.withLiveClock @@ TestAspect.timeout(5.minutes))
    .provideShared(Containers.kafka, ProducerService.live)
