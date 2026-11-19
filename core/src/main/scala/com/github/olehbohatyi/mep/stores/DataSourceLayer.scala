package com.github.olehbohatyi.mep.stores

import zio.*

import javax.sql.DataSource

import com.github.olehbohatyi.mep.config.PostgresConfig
import com.zaxxer.hikari.{ HikariConfig, HikariDataSource }

object DataSourceLayer:

  val live: ZLayer[PostgresConfig, Throwable, DataSource] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[PostgresConfig]
        ds  <- ZIO.fromAutoCloseable(ZIO.attemptBlocking {
                 val hc = HikariConfig()
                 hc.setJdbcUrl(cfg.url)
                 hc.setUsername(cfg.user)
                 hc.setPassword(cfg.password)
                 hc.setMaximumPoolSize(cfg.maxPool)
                 hc.setConnectionTimeout(cfg.connectTimeout.toMillis)
                 // pgjdbc's socketTimeout is in seconds; it also bounds how long one statement may run
                 hc.addDataSourceProperty("socketTimeout", cfg.socketTimeout.toSeconds.max(1L).toString)
                 HikariDataSource(hc)
               })
      yield ds
    }
