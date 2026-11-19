package com.github.olehbohatyi.mep.stores

import zio.*

import scala.jdk.CollectionConverters.*
import com.github.olehbohatyi.mep.config.PostgresConfig
import org.flywaydb.core.Flyway

object Migrations:

  // Applies pending Flyway migrations; safe to run concurrently from several services
  val run: RIO[PostgresConfig, Unit] = from("classpath:db/migration")

  /** Migrates with a connection of its own instead of the app's pool. The pool's `socketTimeout` bounds every
    * statement, and a migration step that runs longer must not fail on it: a failed migration cannot be edited. Opening
    * the connection is still bounded by `connectTimeout`.
    */
  def from(locations: String*): RIO[PostgresConfig, Unit] =
    for
      cfg    <- ZIO.service[PostgresConfig]
      result <- ZIO.attemptBlocking {
                  Flyway
                    .configure()
                    .dataSource(cfg.url, cfg.user, cfg.password)
                    .jdbcProperties(Map("connectTimeout" -> cfg.connectTimeout.toSeconds.max(1L).toString).asJava)
                    .locations(locations*)
                    .load()
                    .migrate()
                }
      _      <- ZIO.logInfo(s"Flyway: ${result.migrationsExecuted} migration(s) applied")
    yield ()
