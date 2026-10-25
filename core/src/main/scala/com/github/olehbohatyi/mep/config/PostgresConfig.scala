package com.github.olehbohatyi.mep.config

final case class PostgresConfig(
    url: String,
    user: String,
    password: String,
    maxPool: Int,
  )
