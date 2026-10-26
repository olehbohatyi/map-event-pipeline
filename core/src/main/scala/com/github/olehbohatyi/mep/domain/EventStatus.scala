package com.github.olehbohatyi.mep.domain

import zio.json.JsonCodec
import zio.schema.{ derived, Schema }
import zio.schema.annotation.simpleEnum

@simpleEnum enum EventStatus derives JsonCodec, Schema:
  case Raw, Enriched, Archived
