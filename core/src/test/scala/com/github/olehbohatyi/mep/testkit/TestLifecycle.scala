package com.github.olehbohatyi.mep.testkit

import zio.*

import com.github.olehbohatyi.mep.config.{ LifecycleConfig, PostgresConfig }

// Event lifetimes for tests: short and different per type, so a test can tell them apart
object TestLifecycle:
  val config: LifecycleConfig =
    LifecycleConfig(hazardTtl = 6.hours, trafficTtl = 2.hours, signTtl = 30.days, sweepInterval = 1.minute)

  // For services that only read the pool size (the expiry sweep stays below it); never connected to
  def pool(maxPool: Int = 10): PostgresConfig =
    PostgresConfig("jdbc:postgresql://unused/unused", "unused", "unused", maxPool)
