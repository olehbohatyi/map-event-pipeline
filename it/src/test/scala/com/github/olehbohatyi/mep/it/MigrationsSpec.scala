package com.github.olehbohatyi.mep.it

import zio.*
import zio.test.*

import javax.sql.DataSource

import com.github.olehbohatyi.mep.config.PostgresConfig
import com.github.olehbohatyi.mep.stores.{ DataSourceLayer, Migrations }

// The pool's socketTimeout bounds every statement the apps run; Flyway runs at startup and must not be bound by it
object MigrationsSpec extends ZIOSpecDefault:

  private val SocketTimeout = 1.second

  def spec = suite("Migrations")(
    test("a migration step longer than the pool's socket timeout still applies") {
      for
        // The same step on the pool fails, so the timeout is in force
        onPool   <- ZIO
                      .serviceWithZIO[DataSource](ds =>
                        ZIO.attemptBlocking {
                          val c = ds.getConnection
                          try c.createStatement().execute("select pg_sleep(3)")
                          finally c.close()
                        }
                      )
                      .exit
        migrated <- Migrations.from("classpath:db/slow").exit
      yield assertTrue(onPool.isFailure, migrated.isSuccess)
    }
  ).provideShared(
    Containers.postgres.project(_.copy(socketTimeout = SocketTimeout)),
    DataSourceLayer.live,
  ) @@ TestAspect.withLiveClock
