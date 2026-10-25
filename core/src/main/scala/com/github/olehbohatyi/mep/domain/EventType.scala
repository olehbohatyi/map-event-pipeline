package com.github.olehbohatyi.mep.domain

import zio.json.JsonCodec

enum EventType derives JsonCodec:
  case Hazard, Sign, Traffic
