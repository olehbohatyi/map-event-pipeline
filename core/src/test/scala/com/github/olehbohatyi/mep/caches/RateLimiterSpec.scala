package com.github.olehbohatyi.mep.caches

import zio.test.*

import com.github.olehbohatyi.mep.config.RateLimitConfig

object RateLimiterSpec extends ZIOSpecDefault:

  def spec = suite("RateLimiter")(
    // Regression: a window of 0 made the script delete each counter at once, so nothing was ever limited
    test("rejects a window that is not positive, unless the limit is off") {
      assertTrue(
        RateLimiter.validate(RateLimitConfig(600, 0)).isLeft,
        RateLimiter.validate(RateLimitConfig(600, -5)).isLeft,
        RateLimiter.validate(RateLimitConfig(0, 0)).isRight,
        RateLimiter.validate(RateLimitConfig(600, 60)) == Right(RateLimitConfig(600, 60)),
      )
    }
  )
