package com.github.olehbohatyi.mep.domain

import zio.json.JsonCodec
import zio.schema.{ derived, Schema }
import zio.schema.annotation.simpleEnum

/** An event is open (`Raw` as the gate publishes it, `Enriched` once the pipe stored it) until it is closed: `Resolved`
  * by a client or `Expired` after its type's lifetime. A closed event never opens again.
  */
@simpleEnum enum EventStatus derives JsonCodec, Schema:
  case Raw, Enriched, Resolved, Expired

  def isClosed: Boolean = this == Resolved || this == Expired
