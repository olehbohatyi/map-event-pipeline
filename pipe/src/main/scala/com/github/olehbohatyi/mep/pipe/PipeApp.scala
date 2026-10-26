package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.kafka.consumer.Consumer

import javax.sql.DataSource

import com.github.olehbohatyi.mep.AppLogging
import com.github.olehbohatyi.mep.broker.{ ProducerService, TopicBootstrap }
import com.github.olehbohatyi.mep.caches.{ GeoCache, IdempotencyCache, RedisService }
import com.github.olehbohatyi.mep.config.{ AppConfig, KafkaConfig }
import com.github.olehbohatyi.mep.geo.RegionResolver
import com.github.olehbohatyi.mep.stores.{ DataSourceLayer, EventRepo, Migrations, RegionRepo }

object PipeApp extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] = AppLogging.live

  def run: ZIO[Any, Throwable, Unit] =
    program.provide(
      AppConfig.all,
      DataSourceLayer.live,
      EventRepo.live,
      ProducerService.live,
      StreamProcessor.consumerSettings,
      Consumer.live,
      RedisService.live,
      IdempotencyCache.live(DeduplicationService.Namespace),
      GeoCache.live,
      RegionRepo.live,
      RegionResolver.live,
      DeduplicationService.live,
      EnrichmentService.live,
      EscalationService.live,
      EventHandler.live,
      RecordHandler.live,
      StreamProcessor.live,
    )

  private val program: RIO[StreamProcessor & DataSource & KafkaConfig, Unit] =
    Migrations.run *> TopicBootstrap.run *> ZIO.logInfo("Pipe started") *>
      ZIO.serviceWithZIO[StreamProcessor](_.run)
