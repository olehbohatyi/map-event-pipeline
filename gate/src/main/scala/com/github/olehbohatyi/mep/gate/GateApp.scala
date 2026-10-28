package com.github.olehbohatyi.mep.gate

import zio.*
import zio.http.*
import zio.telemetry.opentelemetry.tracing.Tracing

import javax.sql.DataSource

import com.github.olehbohatyi.mep.broker.{ ProducerService, TopicBootstrap }
import com.github.olehbohatyi.mep.caches.{ GeoCache, IdempotencyCache, RedisService }
import com.github.olehbohatyi.mep.config.{ AppConfig, HttpConfig, KafkaConfig }
import com.github.olehbohatyi.mep.geo.RegionResolver
import com.github.olehbohatyi.mep.stores.{ DataSourceLayer, EventRepo, Migrations, RegionRepo, StatsRepo, TrendsRepo }
import com.github.olehbohatyi.mep.telemetry.Telemetry

object GateApp extends ZIOApp:

  type Environment = Telemetry.Env
  val environmentTag: EnvironmentTag[Environment]       = EnvironmentTag[Environment]
  val bootstrap: ZLayer[ZIOAppArgs, Any, Telemetry.Env] = Telemetry.bootstrap("gate")

  def run: ZIO[Telemetry.Env, Throwable, Unit] =
    program.provideSome[Telemetry.Env](
      Telemetry.tracing,
      Telemetry.metrics,
      AppConfig.all,
      DataSourceLayer.live,
      EventRepo.live,
      StatsRepo.live,
      TrendsRepo.live,
      ProducerService.live,
      RedisService.live,
      IdempotencyCache.live(IngestionService.Namespace),
      GeoCache.live,
      RegionRepo.live,
      RegionResolver.live,
      IngestionService.live,
      QueryService.live,
      LiveFeed.live,
    )

  private val program
      : RIO[IngestionService & QueryService & LiveFeed & Tracing & HttpConfig & KafkaConfig & DataSource, Unit] =
    for
      _         <- Migrations.run
      _         <- TopicBootstrap.run
      cfg       <- ZIO.service[HttpConfig]
      ingestion <- ZIO.service[IngestionService]
      query     <- ZIO.service[QueryService]
      feed      <- ZIO.service[LiveFeed]
      tracing   <- ZIO.service[Tracing]
      _         <- ZIO.logInfo(s"Gate listening on ${cfg.host}:${cfg.port}")
      // Ending the live streams first lets the server's graceful shutdown finish the remaining requests quickly;
      // otherwise it waits out its timeout on the endless streams (and `docker stop` kills the gate meanwhile)
      _         <- Server
                     .serve(GateRoutes(ingestion, query, feed, tracing))
                     .ensuring(feed.close)
                     .provide(Server.defaultWith(_.binding(cfg.host, cfg.port)))
    yield ()
