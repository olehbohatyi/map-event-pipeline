package com.github.olehbohatyi.mep.stat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{ StreamingQueryListener, Trigger }
import org.apache.spark.sql.streaming.StreamingQueryListener.{
  QueryProgressEvent,
  QueryStartedEvent,
  QueryTerminatedEvent,
}
import org.slf4j.LoggerFactory

// Long-running streaming job, run with spark-submit:
//   spark-submit --class com.github.olehbohatyi.mep.stat.TrendsApp stat.jar
// Reads map.events.enriched and keeps map_event_trends (hourly counts per region and type) up to date.
object TrendsApp:

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit =
    val cfg   = TrendsConfig.fromEnv()
    logger.info(s"TrendsApp starting — kafka=${cfg.kafkaServers} topic=${cfg.topic} checkpoint=${cfg.checkpointDir}")
    val spark = SparkSession
      .builder()
      .appName("map-event-pipeline-trends")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.sql.shuffle.partitions", "4")
      // Hour windows start on UTC hours
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    // A fresh checkpoint starts from the oldest record still retained, so the table is filled from history first
    val records = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cfg.kafkaServers)
      .option("subscribe", cfg.topic)
      .option("startingOffsets", "earliest")
      // If retention deleted records the checkpoint still points to (the job was stopped for longer than the topic
      // keeps data), continue from the oldest record left instead of failing on every restart: the counts for the
      // expired hours are lost either way, and Spark logs a warning
      .option("failOnDataLoss", "false")
      .load()
      .select(col("value").cast("string").as("value"))

    val metrics = TrendsMetrics(StatTelemetry.meter("trends"))
    spark.streams.addListener(ProgressListener(metrics))

    val query = Trends.start(records, cfg.checkpointDir, Trigger.ProcessingTime(cfg.trigger.toMillis)) { (batch, id) =>
      val written = Trends.upsert(cfg.db)(batch)
      metrics.upserted(written)
      if written > 0 then logger.info(s"batch $id: upserted $written hourly count(s)")
    }
    sys.addShutdownHook(query.stop()): Unit
    query.awaitTermination()

  /** Records each batch's numbers in the metrics and logs the events the counts miss: late ones are dropped by design
    * (see Trends.hourly), and records the topic's retention deleted before they were read are skipped. Both leave hours
    * undercounted until TrendsBackfillApp recounts them.
    */
  final private[stat] class ProgressListener(metrics: TrendsMetrics) extends StreamingQueryListener:
    override def onQueryStarted(event: QueryStartedEvent): Unit       = ()
    override def onQueryTerminated(event: QueryTerminatedEvent): Unit =
      event.exception.foreach { error =>
        metrics.failed()
        logger.error(s"trends query stopped: $error")
      }
    override def onQueryProgress(event: QueryProgressEvent): Unit     =
      val progress = event.progress
      val stats    = Trends.batchStats(progress)
      metrics.batch(stats)
      if stats.droppedLate > 0 then
        logger.warn(
          s"batch ${progress.batchId}: dropped ${stats.droppedLate} event(s) older than the ${Trends.Watermark} watermark"
        )
      if stats.skipped > 0 then
        logger.warn(
          s"batch ${progress.batchId}: skipped ${stats.skipped} record(s) that the topic's retention deleted before " +
            "they were read; their hours stay undercounted until TrendsBackfillApp recounts them"
        )
