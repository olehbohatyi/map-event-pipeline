package com.github.olehbohatyi.mep.pipe

import zio.*
import zio.kafka.consumer.{ Consumer, ConsumerSettings }

import javax.sql.DataSource

import com.github.olehbohatyi.mep.AppLogging
import com.github.olehbohatyi.mep.broker.ProducerService
import com.github.olehbohatyi.mep.caches.{ GeoCache, IdempotencyCache, RedisService }
import com.github.olehbohatyi.mep.config.{ AppConfig, KafkaConfig }
import com.github.olehbohatyi.mep.geo.RegionResolver
import com.github.olehbohatyi.mep.stores.{ DataSourceLayer, EventRepo, Migrations }

object PipeApp extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] = AppLogging.live

  private val consumerSettings: URLayer[KafkaConfig, ConsumerSettings] =
    ZLayer.fromFunction((cfg: KafkaConfig) => ConsumerSettings(cfg.servers).withGroupId(cfg.consumerGroup))

  def run: ZIO[Any, Throwable, Unit] =
    program.provide(
      AppConfig.all,
      DataSourceLayer.live,
      EventRepo.live,
      ProducerService.live,
      consumerSettings,
      Consumer.live,
      RedisService.live,
      IdempotencyCache.live(DeduplicationService.Namespace),
      GeoCache.live,
      RegionResolver.live,
      DeduplicationService.live,
      EnrichmentService.live,
      EscalationService.live,
      EventHandler.live,
      StreamProcessor.live,
    )

  private val program: RIO[StreamProcessor & DataSource, Unit] =
    Migrations.run *> ZIO.logInfo("Pipe started") *> ZIO.serviceWithZIO[StreamProcessor](_.run)
