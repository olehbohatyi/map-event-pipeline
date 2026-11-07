package com.github.olehbohatyi.mep.it

import zio.*
import zio.redis.Redis
import zio.test.*

import com.github.olehbohatyi.mep.caches.*
import com.github.olehbohatyi.mep.config.RateLimitConfig
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
    test("the rate limiter counts per client in fixed windows that expire") {
      for
        redis  <- ZIO.service[Redis]
        limiter = RateLimiter(redis, RateLimitConfig(requests = 3, windowSeconds = 2), namespace = "t-rate")
        first  <- ZIO.foreach(1 to 4)(_ => limiter.acquire("10.0.0.1"))
        other  <- limiter.acquire("10.0.0.2")
        ttl    <- redis.pTtl("t-rate:10.0.0.1")
        _      <- ZIO.sleep(2100.millis)
        again  <- limiter.acquire("10.0.0.1")
      yield assertTrue(
        first.take(3).forall(_ == RateLimiter.Decision.Allowed),
        first.last == RateLimiter.Decision.Limited(2.seconds),
        other == RateLimiter.Decision.Allowed,
        // Every counter expires: one left without a TTL would limit its client for good
        ttl.toMillis > 0L && ttl.toMillis <= 2000L,
        again == RateLimiter.Decision.Allowed,
      )
    },
    // Regression guard for the counter's TTL: a key that lost it (written before the script set one) gets it back
    test("a rate limit counter without an expiry gets one on the next request") {
      for
        redis  <- ZIO.service[Redis]
        _      <- redis.set("t-rate:10.0.0.9", "7")
        limiter = RateLimiter(redis, RateLimitConfig(requests = 3, windowSeconds = 60), namespace = "t-rate")
        result <- limiter.acquire("10.0.0.9")
        ttl    <- redis.pTtl("t-rate:10.0.0.9")
      yield assertTrue(result == RateLimiter.Decision.Limited(60.seconds), ttl.toMillis > 0L)
    },
    test("rate limit counters use their own keyspace") {
      assertTrue(
        Set(RateLimiter.Namespace, IngestionService.Namespace, DeduplicationService.Namespace).size == 3
      )
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
