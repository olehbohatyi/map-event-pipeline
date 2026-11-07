package com.github.olehbohatyi.mep.stat

import java.time.Instant

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scala.util.control.NonFatal

// One-shot job that recounts hourly trends from map_events, run with spark-submit:
//   spark-submit --class com.github.olehbohatyi.mep.stat.TrendsBackfillApp stat.jar [--from <instant|date>] [--to …]
// Without arguments it recounts every hour that ended an hour or more ago.
object TrendsBackfillApp:

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit =
    TrendsBackfill.parseArgs(args.toSeq) match
      case Left(error)       =>
        logger.error(error)
        sys.exit(2)
      case Right((from, to)) =>
        val cfg   = StatConfig.fromEnv()
        val range = TrendsBackfill.range(from, to, Instant.now())
        logger.info(
          s"TrendsBackfillApp starting — hours from ${range.from.fold("the first event")(_.toString)} to ${range.to}"
        )
        val spark = SparkSession
          .builder()
          .appName("map-event-pipeline-trends-backfill")
          .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
          .config("spark.sql.shuffle.partitions", "8")
          // Hour windows start on UTC hours, as in TrendsApp
          .config("spark.sql.session.timeZone", "UTC")
          .getOrCreate()
        try
          val written = TrendsBackfill.run(spark, cfg, range)
          logger.info(s"TrendsBackfillApp finished — $written hourly count(s) upserted into map_event_trends")
        catch
          case NonFatal(ex) =>
            logger.error(s"TrendsBackfillApp failed: ${ex.getMessage}", ex)
            sys.exit(1)
        finally spark.stop()
