package com.github.olehbohatyi.mep.stat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
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
      .load()
      .select(col("value").cast("string").as("value"))

    val query = Trends.start(records, cfg.checkpointDir, Trigger.ProcessingTime(cfg.trigger.toMillis)) { (batch, id) =>
      val written = Trends.upsert(cfg.db)(batch)
      if written > 0 then logger.info(s"batch $id: upserted $written hourly count(s)")
    }
    sys.addShutdownHook(query.stop()): Unit
    query.awaitTermination()
