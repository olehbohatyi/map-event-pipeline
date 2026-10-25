package com.github.olehbohatyi.mep.domain

import zio.json.JsonCodec

enum Severity derives JsonCodec:
  case Low, Medium, High, Critical
