package com.github.olehbohatyi.mep.stores

import zio.*

import javax.sql.DataSource

import org.flywaydb.core.Flyway

object Migrations:

  // Applies pending Flyway migrations; safe to run concurrently from several services
  val run: RIO[DataSource, Unit] =
    for
      ds     <- ZIO.service[DataSource]
      result <- ZIO.attemptBlocking {
                  Flyway
                    .configure()
                    .dataSource(ds)
                    .locations("classpath:db/migration")
                    .load()
                    .migrate()
                }
      _      <- ZIO.logInfo(s"Flyway: ${result.migrationsExecuted} migration(s) applied")
    yield ()
