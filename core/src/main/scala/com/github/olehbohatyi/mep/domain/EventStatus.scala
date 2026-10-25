package com.github.olehbohatyi.mep.domain

import zio.json.JsonCodec

enum EventStatus derives JsonCodec:
  case Raw, Enriched, Archived
