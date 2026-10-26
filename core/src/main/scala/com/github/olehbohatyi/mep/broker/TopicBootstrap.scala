package com.github.olehbohatyi.mep.broker

import zio.*
import zio.kafka.admin.{ AdminClient, AdminClientSettings }
import zio.kafka.admin.AdminClient.NewTopic

import org.apache.kafka.common.errors.{
  InvalidConfigurationException,
  InvalidPartitionsException,
  InvalidReplicationFactorException,
  InvalidTopicException,
  TopicAuthorizationException,
  TopicExistsException,
}
import com.github.olehbohatyi.mep.config.KafkaConfig

// Creates the pipeline's topics before any service produces or consumes. Brokers run with
// auto-create disabled, so partitions and retention are decided here, not by broker defaults.
object TopicBootstrap:

  // The broker may still be starting when the apps come up, so connection errors are retried.
  // A broker that answers with a rejection of the topic itself (e.g. a replication factor above
  // its broker count) will answer the same way every time, so those fail immediately.
  def isPermanent(err: Throwable): Boolean =
    Iterator.iterate(err)(_.getCause).takeWhile(_ != null).exists {
      case _: InvalidReplicationFactorException | _: InvalidPartitionsException | _: InvalidConfigurationException |
           _: InvalidTopicException | _: TopicAuthorizationException =>
        true
      case _ => false
    }

  private val retryPolicy =
    (Schedule.exponential(500.millis) && Schedule.recurs(8)).whileInput[Throwable](!isPermanent(_))

  def newTopics(cfg: KafkaConfig): List[NewTopic] =
    val s                                        = cfg.topicSettings
    def topic(name: String, retentionHours: Int) =
      NewTopic(
        name,
        s.partitions,
        s.replicationFactor.toShort,
        Map("retention.ms" -> (retentionHours.toLong * 3600 * 1000).toString),
      )
    cfg.topics.all.map(name => topic(name, if name == cfg.topics.dlq then s.dlqRetentionHours else s.retentionHours))

  val run: RIO[KafkaConfig, Unit] =
    ZIO.serviceWithZIO[KafkaConfig] { cfg =>
      val problems = cfg.topicSettings.problems
      ZIO
        .fail(IllegalArgumentException(s"Invalid mep.kafka.topic-settings: ${problems.mkString("; ")}"))
        .when(problems.nonEmpty) *>
        ZIO
          .scoped {
            for
              admin    <- AdminClient.make(AdminClientSettings(cfg.servers))
              existing <- admin.listTopics().map(_.keySet)
              missing   = newTopics(cfg).filterNot(t => existing.contains(t.name))
              // gate and pipe may race to create the same topic; losing that race is fine
              created  <-
                ZIO.filter(missing)(t =>
                  admin.createTopic(t).as(true).catchSome { case _: TopicExistsException => ZIO.succeed(false) }
                )
              _        <- ZIO.logInfo(
                            if created.isEmpty then "Kafka topics already exist"
                            else s"Created Kafka topics: ${created.map(_.name).mkString(", ")}"
                          )
            yield ()
          }
          .tapError(err =>
            if isPermanent(err) then ZIO.logError(s"Topic bootstrap failed: ${err.getMessage}")
            else ZIO.logWarning(s"Topic bootstrap failed, retrying: ${err.getMessage}")
          )
          .retry(retryPolicy)
          .unit
    }
