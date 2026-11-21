package com.github.olehbohatyi.mep.gate

import zio.*
import zio.http.*
import zio.telemetry.opentelemetry.tracing.Tracing

import com.github.olehbohatyi.mep.broker.{ ProducerService, TopicBootstrap }
import com.github.olehbohatyi.mep.caches.{ GeoCache, IdempotencyCache, RateLimiter, RedisService }
import com.github.olehbohatyi.mep.config.{ AppConfig, HttpConfig, KafkaConfig, PostgresConfig }
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
      ZLayer.fromFunction((http: HttpConfig) => http.rateLimit),
      RateLimiter.live,
      ApiKeys.live,
      GeoCache.live,
      RegionRepo.live,
      RegionResolver.live,
      IngestionService.live,
      ResolutionService.live,
      QueryService.live,
      LiveFeed.live,
    )

  private val program: RIO[
    IngestionService & ResolutionService & QueryService & LiveFeed & ApiKeys & RateLimiter & Tracing & HttpConfig &
      KafkaConfig & PostgresConfig,
    Unit,
  ] =
    for
      _          <- Migrations.run
      _          <- TopicBootstrap.run
      cfg        <- ZIO.service[HttpConfig]
      ingestion  <- ZIO.service[IngestionService]
      resolution <- ZIO.service[ResolutionService]
      query      <- ZIO.service[QueryService]
      feed       <- ZIO.service[LiveFeed]
      keys       <- ZIO.service[ApiKeys]
      limiter    <- ZIO.service[RateLimiter]
      tracing    <- ZIO.service[Tracing]
      _          <- ZIO.logInfo(s"Gate listening on ${cfg.host}:${cfg.port}")
      _          <- ZIO.logWarning("API keys are not required: anyone can write").unless(keys.required)
      // Ending the live streams first lets the server's graceful shutdown finish the remaining requests quickly;
      // otherwise it waits out its timeout on the endless streams (and `docker stop` kills the gate meanwhile)
      _          <- Server
                      .serve(GateRoutes(ingestion, resolution, query, feed, keys, limiter, tracing))
                      .ensuring(feed.close)
                      .provide(Server.defaultWith(_.binding(cfg.host, cfg.port)))
    yield ()
