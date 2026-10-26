package com.github.olehbohatyi.mep.gate

import zio.*
import zio.http.*

import javax.sql.DataSource

import com.github.olehbohatyi.mep.AppLogging
import com.github.olehbohatyi.mep.broker.{ ProducerService, TopicBootstrap }
import com.github.olehbohatyi.mep.caches.{ GeoCache, IdempotencyCache, RedisService }
import com.github.olehbohatyi.mep.config.{ AppConfig, HttpConfig, KafkaConfig }
import com.github.olehbohatyi.mep.geo.RegionResolver
import com.github.olehbohatyi.mep.stores.{ DataSourceLayer, EventRepo, Migrations, RegionRepo, StatsRepo }

object GateApp extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] = AppLogging.live

  def run: ZIO[Any, Throwable, Unit] =
    program.provide(
      AppConfig.all,
      DataSourceLayer.live,
      EventRepo.live,
      StatsRepo.live,
      ProducerService.live,
      RedisService.live,
      IdempotencyCache.live(IngestionService.Namespace),
      GeoCache.live,
      RegionRepo.live,
      RegionResolver.live,
      IngestionService.live,
      QueryService.live,
    )

  private val program: RIO[IngestionService & QueryService & HttpConfig & KafkaConfig & DataSource, Unit] =
    for
      _         <- Migrations.run
      _         <- TopicBootstrap.run
      cfg       <- ZIO.service[HttpConfig]
      ingestion <- ZIO.service[IngestionService]
      query     <- ZIO.service[QueryService]
      _         <- ZIO.logInfo(s"Gate listening on ${cfg.host}:${cfg.port}")
      _         <- Server
                     .serve(GateRoutes(ingestion, query))
                     .provide(Server.defaultWith(_.binding(cfg.host, cfg.port)))
    yield ()
