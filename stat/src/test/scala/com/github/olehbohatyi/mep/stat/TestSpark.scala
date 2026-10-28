package com.github.olehbohatyi.mep.stat

import zio.*

import org.apache.spark.sql.SparkSession

// One local Spark session for every spec in the test JVM. getOrCreate hands all of them the same session, so no
// spec may stop it while another still runs; it ends with the JVM.
object TestSpark:

  private lazy val session: SparkSession =
    SparkSession
      .builder()
      .master("local[2]")
      .appName("stat-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

  val layer: TaskLayer[SparkSession] = ZLayer(ZIO.attemptBlocking(session))
