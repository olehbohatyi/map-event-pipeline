package com.github.olehbohatyi.mep.it

import zio.*
import zio.redis.Redis
import zio.test.*

import com.github.olehbohatyi.mep.caches.*
import com.github.olehbohatyi.mep.config.KafkaConfig
import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.pipe.EscalationService
import com.github.olehbohatyi.mep.testkit.Samples

object RedisSpec extends ZIOSpecDefault:

  private val withRedis = Containers.redis >>> RedisService.live

  def spec = (suite("Redis caches")(
    test("tryMark is set-if-absent and expires") {
      for
        cache <- ZIO.serviceWith[Redis](IdempotencyCache("t1", _))
        a     <- cache.tryMark("k", 1.second)
        b     <- cache.tryMark("k", 1.second)
        _     <- ZIO.sleep(1500.millis)
        c     <- cache.tryMark("k", 1.second)
      yield assertTrue(a, !b, c)
    },
    test("geo cache round-trips regions") {
      for
        cache <- ZIO.service[GeoCache]
        miss  <- cache.get(GeoPoint(1.23456, 2.34567))
        _     <- cache.put(GeoPoint(1.23456, 2.34567), "airport", 1.minute)
        hit   <- cache.get(GeoPoint(1.23459, 2.34568))
      yield assertTrue(miss.isEmpty, hit.contains("airport"))
    },
    test("escalates Low to Medium once a region sees a burst") {
      val events = List.fill(EscalationService.Threshold.toInt)(
        Samples.event(eventType = EventType.Sign, severity = Severity.Low, region = Some("zone_burst"))
      )
      for
        svc       <- ZIO.service[EscalationService]
        escalated <- ZIO.foreach(events)(svc.escalate)
        severities = escalated.map(_.severity)
      yield assertTrue(severities.init.forall(_ == Severity.Low), severities.last == Severity.Medium)
    },
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock)
    .provideShared(
      withRedis,
      GeoCache.live,
      ZLayer.succeed(KafkaConfig("unused:9092", "g", Containers.topics)),
      EscalationService.live,
    )
