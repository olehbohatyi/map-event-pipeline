package com.github.olehbohatyi.mep.stat

// stat runs under spark-submit, outside the ZIO apps, so it reads plain env vars
final case class StatConfig(
    jdbcUrl: String,
    jdbcUser: String,
    jdbcPassword: String,
  )

object StatConfig:
  def fromEnv(env: Map[String, String] = sys.env): StatConfig =
    StatConfig(
      jdbcUrl = env.getOrElse("POSTGRES_URL", "jdbc:postgresql://localhost:5432/mapevents"),
      jdbcUser = env.getOrElse("POSTGRES_USER", "mep"),
      jdbcPassword = env.getOrElse("POSTGRES_PASSWORD", ""),
    )
