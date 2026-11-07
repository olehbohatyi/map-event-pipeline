package com.github.olehbohatyi.mep.stat

import java.time.{ Instant, LocalDate, ZoneOffset }
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions.*

/** Recounts map_event_trends from map_events, the record of every stored event, for hours the stream could not count in
  * full: records the topic's retention deleted while the job was stopped, or events dropped as late. The counts are
  * absolute and upserted like the stream's, so a recount can run while the stream does.
  */
object TrendsBackfill:

  /** Hours `[from, to)`, both on hour boundaries. */
  final case class Range(from: Option[Instant], to: Instant)

  /** Whole hours only (a partial hour would overwrite its full count with a smaller one), and only hours that ended an
    * hour or more ago: the stream may still update a later hour, with its own count, after the recount wrote it.
    */
  def range(from: Option[Instant], to: Option[Instant], now: Instant): Range =
    val closed = now.truncatedTo(ChronoUnit.HOURS).minus(1, ChronoUnit.HOURS)
    Range(
      from.map(_.truncatedTo(ChronoUnit.HOURS)),
      to.map(_.truncatedTo(ChronoUnit.HOURS)).fold(closed)(Ordering[Instant].min(_, closed)),
    )

  /** `--from` and `--to` take an instant (`2026-10-28T06:00:00Z`) or a UTC date (`2026-10-28`); both are optional. */
  def parseArgs(args: Seq[String]): Either[String, (Option[Instant], Option[Instant])] =
    def instant(value: String): Either[String, Instant] =
      scala
        .util
        .Try(Instant.parse(value))
        .orElse(scala.util.Try(LocalDate.parse(value).atStartOfDay(ZoneOffset.UTC).toInstant))
        .toOption
        .toRight(s"not an instant or a date: $value")

    def loop(rest: List[String], from: Option[Instant], to: Option[Instant])
        : Either[String, (Option[Instant], Option[Instant])] =
      rest match
        case Nil                   => Right((from, to))
        case "--from" :: v :: tail => instant(v).flatMap(i => loop(tail, Some(i), to))
        case "--to" :: v :: tail   => instant(v).flatMap(i => loop(tail, from, Some(i)))
        case other :: _            => Left(s"unexpected argument: $other (usage: [--from <instant|date>] [--to <instant|date>])")

    loop(args.toList, None, None)

  /** The hourly counts of `events` (`event_type`, `region`, `status`, `created_at`, as in map_events) within `range`:
    * enriched events with a region, as the pipe publishes them to the stream.
    */
  def recount(events: DataFrame, range: Range): DataFrame =
    val inRange = col("created_at") < lit(java.sql.Timestamp.from(range.to)) &&
      range.from.fold(lit(true))(f => col("created_at") >= lit(java.sql.Timestamp.from(f)))
    Trends.counts(
      events
        .filter(col("status") === "Enriched" && col("region").isNotNull && inRange)
        .select(col("event_type").as("eventType"), col("region"), col("created_at").as("createdAt"))
    )

  def run(spark: SparkSession, db: StatConfig, range: Range): Int =
    Trends.upsert(db)(recount(readEvents(spark, db, range), range))

  // Only the rows in range leave Postgres; the instants are formatted here, not taken from input as text. One JDBC
  // partition: fine for a one-shot job over this table, a much wider range would want `partitionColumn` on created_at
  private def readEvents(spark: SparkSession, db: StatConfig, range: Range): DataFrame =
    val where = (range.from.map(f => s"created_at >= '$f'::timestamptz").toList :+
      s"created_at < '${range.to}'::timestamptz").mkString(" AND ")
    spark
      .read
      .format("jdbc")
      .option("url", db.jdbcUrl)
      .option("user", db.jdbcUser)
      .option("password", db.jdbcPassword)
      .option("driver", "org.postgresql.Driver")
      .option("query", s"SELECT event_type, region, status, created_at FROM map_events WHERE $where")
      .load()
