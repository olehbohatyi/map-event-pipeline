package com.github.olehbohatyi.mep.it

import zio.*
import zio.redis.Redis
import zio.test.*

import com.github.olehbohatyi.mep.caches.*
import com.github.olehbohatyi.mep.domain.*
import com.github.olehbohatyi.mep.gate.IngestionService
import com.github.olehbohatyi.mep.pipe.{ DeduplicationService, EscalationService }
import com.github.olehbohatyi.mep.testkit.{ Samples, TestKafka }

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
    test("gate idempotency keys and pipe processed keys do not collide") {
      val id = Samples.event().id.toString
      for
        redis     <- ZIO.service[Redis]
        gate       = IdempotencyCache(IngestionService.Namespace, redis)
        pipe       = IdempotencyCache(DeduplicationService.Namespace, redis)
        _         <- gate.mark(id, 1.minute)
        processed <- pipe.isMarked(id)
      yield assertTrue(!processed)
    },
    test("geo cache round-trips regions") {
      for
        cache <- ZIO.service[GeoCache]
        miss  <- cache.get(GeoPoint(1.23456, 2.34567))
        _     <- cache.put(GeoPoint(1.23456, 2.34567), "airport", 1.minute)
        hit   <- cache.get(GeoPoint(1.23459, 2.34568))
      yield assertTrue(miss.isEmpty, hit.contains("airport"))
    },
    test("geo cache never serves a value written by an older resolver") {
      // How the previous release stored a resolved point: no version in the key, a grid zone as the value
      val legacyKey = "mep:geo:3.1416:2.7183"
      for
        redis  <- ZIO.service[Redis]
        cache  <- ZIO.service[GeoCache]
        _      <- redis.set(legacyKey, "zone_3_2", expireTime = Some(1.minute))
        served <- cache.get(GeoPoint(3.1416, 2.7183))
      yield assertTrue(served.isEmpty)
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
      ZLayer.succeed(TestKafka.config()),
      EscalationService.live,
    )
