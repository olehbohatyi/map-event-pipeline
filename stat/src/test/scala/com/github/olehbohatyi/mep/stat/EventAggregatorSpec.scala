package com.github.olehbohatyi.mep.stat

import zio.*
import zio.test.*

import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.*
import scala.jdk.CollectionConverters.*

object EventAggregatorSpec extends ZIOSpecDefault:

  private val schema = StructType(
    Seq(
      StructField("region", StringType, nullable = true),
      StructField("event_type", StringType),
      StructField("status", StringType),
    )
  )

  def spec = suite("EventAggregator")(
    // Resolved and expired events were stored and enriched: closing them must not lower the counts
    test("counts stored events per region and type, closed ones included") {
      for
        session <- ZIO.service[SparkSession]
        rows     = List(
                     Row("zone_1_2", "Hazard", "Enriched"),
                     Row("zone_1_2", "Hazard", "Enriched"),
                     Row("zone_1_2", "Sign", "Enriched"),
                     Row("zone_1_2", "Sign", "Resolved"),
                     Row("zone_1_2", "Sign", "Expired"),
                     Row("zone_3_4", "Hazard", "Enriched"),
                     Row("zone_3_4", "Hazard", "Raw"),
                     Row(null, "Traffic", "Enriched"),
                   )
        result  <- ZIO.attemptBlocking {
                     val df = session.createDataFrame(rows.asJava, schema)
                     EventAggregator
                       .aggregate(df)
                       .collect()
                       .map(r => (r.getAs[String]("region"), r.getAs[String]("event_type"), r.getAs[Long]("count")))
                       .toSet
                   }
      yield assertTrue(result == Set(("zone_1_2", "Hazard", 2L), ("zone_1_2", "Sign", 3L), ("zone_3_4", "Hazard", 1L)))
    }
  ).provideLayerShared(TestSpark.layer) @@ TestAspect.withLiveClock
