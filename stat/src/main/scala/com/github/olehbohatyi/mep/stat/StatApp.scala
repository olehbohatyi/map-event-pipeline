package com.github.olehbohatyi.mep.stat

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scala.util.control.NonFatal

// Batch job, run with spark-submit:
//   spark-submit --class com.github.olehbohatyi.mep.stat.StatApp stat.jar
// For local runs (`sbt stat/run`) the master defaults to local[*].
object StatApp:

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit =
    val cfg   = StatConfig.fromEnv()
    logger.info(s"StatApp starting — jdbc=${cfg.jdbcUrl}")
    val spark = SparkSession
      .builder()
      .appName("map-event-pipeline-stat")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    try
      val rows = EventAggregator.run(spark, cfg)
      logger.info(s"StatApp finished — $rows stat row(s) written to map_event_stats")
    catch
      case NonFatal(ex) =>
        logger.error(s"StatApp failed: ${ex.getMessage}", ex)
        sys.exit(1)
    finally spark.stop()
