package com.github.olehbohatyi.mep.stat

import java.sql.DriverManager

import org.apache.spark.sql.{ DataFrame, SaveMode, SparkSession }
import org.apache.spark.sql.functions.*
import scala.util.Using

// Reads map_events, counts enriched events per region and event type, and upserts
// the totals into map_event_stats. Uses only the untyped DataFrame API: Dataset
// encoders rely on Scala 2 TypeTags, which Scala 3 does not provide.
object EventAggregator:

  private val StagingTable = "map_event_stats_staging"

  def run(spark: SparkSession, cfg: StatConfig): Long =
    val jdbc  = jdbcOptions(cfg)
    val stats = aggregate(readEvents(spark, jdbc)).cache()
    val rows  = stats.count()
    writeStaging(stats, jdbc)
    upsertFromStaging(cfg)
    rows

  def aggregate(events: DataFrame): DataFrame =
    events
      .filter(col("status") === "Enriched" && col("region").isNotNull)
      .groupBy(col("region"), col("event_type"))
      .agg(count(lit(1)).alias("count"))
      .withColumn("updated_at", current_timestamp())

  private def jdbcOptions(cfg: StatConfig): Map[String, String] =
    Map(
      "url"      -> cfg.jdbcUrl,
      "user"     -> cfg.jdbcUser,
      "password" -> cfg.jdbcPassword,
      "driver"   -> "org.postgresql.Driver",
    )

  private def readEvents(spark: SparkSession, jdbc: Map[String, String]): DataFrame =
    spark
      .read
      .format("jdbc")
      .options(jdbc)
      .option("dbtable", "(SELECT region, event_type, status FROM map_events) AS e")
      .load()

  private def writeStaging(stats: DataFrame, jdbc: Map[String, String]): Unit =
    stats
      .write
      .format("jdbc")
      .options(jdbc)
      .option("dbtable", StagingTable)
      .option("truncate", "true")
      .mode(SaveMode.Overwrite)
      .save()

  private def upsertFromStaging(cfg: StatConfig): Unit =
    Using.resource(DriverManager.getConnection(cfg.jdbcUrl, cfg.jdbcUser, cfg.jdbcPassword)) { conn =>
      conn.setAutoCommit(false)
      Using.resource(conn.createStatement()) { stmt =>
        stmt.executeUpdate(
          s"""INSERT INTO map_event_stats (region, event_type, count, updated_at)
             |SELECT region, event_type, count, updated_at FROM $StagingTable
             |ON CONFLICT (region, event_type)
             |DO UPDATE SET count = EXCLUDED.count, updated_at = EXCLUDED.updated_at
             |""".stripMargin
        ): Unit
      }
      conn.commit()
    }
