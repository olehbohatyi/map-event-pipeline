package com.github.olehbohatyi.mep.caches

import zio.*
import zio.test.*

import com.github.olehbohatyi.mep.caches.RedisService.{ bounded, CommandTimeout, RedisTimeout }

object RedisServiceSpec extends ZIOSpecDefault:

  def spec = suite("RedisService")(
    // Regression: zio-redis waits for an answer forever, so a frozen Redis hung the pipe instead of stopping it
    test("a command Redis never answers fails after the command timeout") {
      for
        command <- ZIO.never.bounded.fork
        _       <- TestClock.adjust(CommandTimeout)
        exit    <- command.await
      yield assertTrue(exit.causeOption.flatMap(_.failureOption).contains(RedisTimeout(CommandTimeout)))
    }
  )
