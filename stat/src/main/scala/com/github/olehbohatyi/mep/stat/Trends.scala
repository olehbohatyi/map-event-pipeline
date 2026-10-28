package com.github.olehbohatyi.mep.stat

import java.sql.DriverManager

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.streaming.{ OutputMode, StreamingQuery, Trigger }
import org.apache.spark.sql.types.*
import scala.util.Using

// Hourly event counts per region and type, as a stream over what the pipe publishes to map.events.enriched.
// Source and sink are parameters so tests can feed text files and collect batches without Kafka or Postgres;
// only the untyped DataFrame API is used (Dataset encoders need Scala 2 TypeTags).
object Trends:

  // The fields of the pipe's MapEvent JSON that the counts need
  val eventSchema: StructType = StructType(
    Seq(
      StructField("id", StringType),
      StructField("eventType", StringType),
      StructField("region", StringType),
      StructField("createdAt", StringType),
    )
  )

  // Events whose receive time is this far behind the newest one seen are dropped as late
  val Watermark = "10 minutes"

  // `values` has a string column `value` holding one MapEvent each (a Kafka record or a line of a file).
  // Unreadable records and events without a region yet are skipped.
  def parse(values: DataFrame): DataFrame =
    values
      .select(from_json(col("value"), eventSchema).as("e"))
      .select("e.*")
      .withColumn("createdAt", to_timestamp(col("createdAt")))
      .filter(col("id").isNotNull && col("createdAt").isNotNull && col("region").isNotNull)

  /** Windows by `createdAt`, the gate's receive time, not the client's `occurredAt`: the watermark follows the newest
    * event time seen, so one client with a wrong clock could otherwise mark every later event as late. The pipe may
    * publish an event twice after a retry; duplicates are dropped by id while the watermark keeps their state.
    */
  def hourly(events: DataFrame): DataFrame =
    events
      .withWatermark("createdAt", Watermark)
      .dropDuplicatesWithinWatermark("id")
      .groupBy(window(col("createdAt"), "1 hour"), col("region"), col("eventType"))
      .count()
      .select(col("window.start").as("window_start"), col("region"), col("eventType").as("event_type"), col("count"))

  /** Starts the query. In update mode each micro-batch holds the windows that changed, with their full counts, so the
    * sink can upsert them: replaying a batch after a crash writes the same values again.
    */
  def start(values: DataFrame, checkpointDir: String, trigger: Trigger)(sink: (DataFrame, Long) => Unit)
      : StreamingQuery =
    hourly(parse(values))
      .writeStream
      .queryName("trends")
      .outputMode(OutputMode.Update())
      .option("checkpointLocation", checkpointDir)
      .trigger(trigger)
      .foreachBatch((batch: DataFrame, batchId: Long) => sink(batch, batchId))
      .start()

  // window_start goes into a timestamptz column, so the JDBC driver keeps the instant whatever the JVM's time zone
  def upsert(db: StatConfig)(batch: DataFrame): Int =
    val rows: Array[Row] = batch.collect()
    if rows.isEmpty then 0
    else
      Using.resource(DriverManager.getConnection(db.jdbcUrl, db.jdbcUser, db.jdbcPassword)) { conn =>
        Using.resource(
          conn.prepareStatement(
            """INSERT INTO map_event_trends (window_start, region, event_type, count, updated_at)
              |VALUES (?, ?, ?, ?, NOW())
              |ON CONFLICT (window_start, region, event_type)
              |DO UPDATE SET count = EXCLUDED.count, updated_at = EXCLUDED.updated_at""".stripMargin
          )
        ) { st =>
          rows.foreach { r =>
            st.setTimestamp(1, r.getTimestamp(0))
            st.setString(2, r.getString(1))
            st.setString(3, r.getString(2))
            st.setLong(4, r.getLong(3))
            st.addBatch()
          }
          st.executeBatch().length
        }
      }
