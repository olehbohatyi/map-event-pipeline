package com.github.olehbohatyi.mep

import zio.*
import zio.logging.backend.SLF4J

// Routes ZIO logs through SLF4J/logback
object AppLogging:
  val live: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j
