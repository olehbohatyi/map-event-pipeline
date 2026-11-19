package com.github.olehbohatyi.mep.config

import zio.*

/** `connectTimeout` bounds the wait for a pooled connection, `socketTimeout` a statement on one. Without them a
  * Postgres that stops answering held each attempt for 30 s (the pool's default) and a statement already running for as
  * long as TCP keeps the connection, so the pipe hung instead of stopping to be restarted.
  */
final case class PostgresConfig(
    url: String,
    user: String,
    password: String,
    maxPool: Int,
    connectTimeout: Duration = 5.seconds,
    socketTimeout: Duration = 30.seconds,
  )
