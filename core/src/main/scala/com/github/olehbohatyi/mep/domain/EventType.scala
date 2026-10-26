package com.github.olehbohatyi.mep.domain

import zio.json.JsonCodec
import zio.schema.{ derived, Schema }
import zio.schema.annotation.simpleEnum

@simpleEnum enum EventType derives JsonCodec, Schema:
  case Hazard, Sign, Traffic
