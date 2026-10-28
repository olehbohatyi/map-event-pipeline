import sbt.*

// Single source of versions for the sbt build — keep build.mill in sync
object Versions {
  val scala      = "3.10.0"
  val scalaSpark = "3.7.4" // scala-steward:off — last Scala 3 on the 2.13 stdlib that Spark 4.x links against
  val zio        = "2.1.27"
  val zioConfig  = "4.1.0"
  val zioKafka   = "3.8.1"
  val zioHttp    = "3.11.6"
  val zioRedis   = "1.3.0"
  val zioJson    = "1.1.1"
  val zioSchema  = "1.8.7"
  val zioOtel    = "3.1.21"
  val otel       = "1.67.0"
  val zioLogging = "2.5.3"
  val quill      = "4.8.6"
  val postgres   = "42.7.14"
  val hikari     = "7.1.0"
  val flyway     = "13.11.1"
  val logback    = "1.6.6"
  val chimney    = "2.1.0"
  val spark      = "4.2.0"
  val testcont   = "2.0.5"
  val gatling    = "3.16.0"
}

object Dependencies {

  val zio = Seq(
    "dev.zio" %% "zio"         % Versions.zio,
    "dev.zio" %% "zio-streams" % Versions.zio,
  )

  val zioConfig = Seq(
    "dev.zio" %% "zio-config"          % Versions.zioConfig,
    "dev.zio" %% "zio-config-magnolia" % Versions.zioConfig,
    "dev.zio" %% "zio-config-typesafe" % Versions.zioConfig,
  )

  val zioLogging = Seq(
    "dev.zio" %% "zio-logging"        % Versions.zioLogging,
    "dev.zio" %% "zio-logging-slf4j2" % Versions.zioLogging,
  )

  val zioKafka            = "dev.zio" %% "zio-kafka"             % Versions.zioKafka
  val zioHttp             = "dev.zio" %% "zio-http"              % Versions.zioHttp
  val zioRedis            = "dev.zio" %% "zio-redis"             % Versions.zioRedis
  val zioJson             = "dev.zio" %% "zio-json"              % Versions.zioJson
  val zioSchema           = "dev.zio" %% "zio-schema"            % Versions.zioSchema
  val zioSchemaDerivation = "dev.zio" %% "zio-schema-derivation" % Versions.zioSchema

  // Quill pulls zio-json 0.7 for JSON columns (unused here); it clashes with zio-json 1.x
  val quill    = "io.getquill"     %% "quill-jdbc-zio"             % Versions.quill exclude ("dev.zio", "zio-json_3")
  val postgres = "org.postgresql"   % "postgresql"                 % Versions.postgres
  val hikari   = "com.zaxxer"       % "HikariCP"                   % Versions.hikari
  val flyway   = "org.flywaydb"     % "flyway-core"                % Versions.flyway
  val flywayPg = "org.flywaydb"     % "flyway-database-postgresql" % Versions.flyway
  val logback  = "ch.qos.logback"   % "logback-classic"            % Versions.logback
  val chimney  = "io.scalaland"    %% "chimney"                    % Versions.chimney
  val sparkSql = "org.apache.spark" % "spark-sql_2.13"             % Versions.spark

  // The Kafka source is not in the apache/spark image, so it goes into stat.jar — without what the image already
  // ships at the same versions (Hadoop client, compression codecs, slf4j, Scala, spark-tags)
  val sparkKafka = ("org.apache.spark" % "spark-sql-kafka-0-10_2.13" % Versions.spark).excludeAll(
    ExclusionRule("org.apache.hadoop"),
    ExclusionRule("at.yawk.lz4"),
    ExclusionRule("org.xerial.snappy"),
    ExclusionRule("org.slf4j"),
    ExclusionRule("com.google.code.findbugs"),
    ExclusionRule("org.scala-lang"),
    ExclusionRule("org.scala-lang.modules"),
    ExclusionRule("org.apache.spark", "spark-tags_2.13"),
  )

  // Traces, metrics and logs over OTLP: the SDK is configured from the standard OTEL_* environment variables
  val telemetry   = Seq(
    "dev.zio"         %% "zio-opentelemetry"                         % Versions.zioOtel,
    "io.opentelemetry" % "opentelemetry-sdk"                         % Versions.otel,
    "io.opentelemetry" % "opentelemetry-exporter-otlp"               % Versions.otel,
    "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure" % Versions.otel,
  )
  // In-memory exporters, to assert span structure in tests
  val otelTesting = "io.opentelemetry" % "opentelemetry-sdk-testing" % Versions.otel % Test

  val zioTest = Seq(
    "dev.zio" %% "zio-test"     % Versions.zio % Test,
    "dev.zio" %% "zio-test-sbt" % Versions.zio % Test,
  )

  val testcontainers = Seq(
    "org.testcontainers" % "testcontainers-postgresql" % Versions.testcont % Test,
    "org.testcontainers" % "testcontainers-kafka"      % Versions.testcont % Test,
  )

  val coreDeps: Seq[ModuleID] =
    zio ++ zioConfig ++ zioLogging ++
      Seq(
        zioKafka,
        zioRedis,
        zioJson,
        zioSchema,
        zioSchemaDerivation,
        quill,
        postgres,
        hikari,
        flyway,
        flywayPg,
        logback,
        chimney,
        otelTesting,
      ) ++
      telemetry ++
      zioTest

  val gateDeps: Seq[ModuleID] = Seq(zioHttp) ++ zioTest
  val pipeDeps: Seq[ModuleID] = zioTest
  val itDeps: Seq[ModuleID]   = zioTest ++ testcontainers
  // Gatling is built on the Scala 2.13 library; its Java DSL is used from Scala 3
  val loadDeps: Seq[ModuleID] = Seq("io.gatling.highcharts" % "gatling-charts-highcharts" % Versions.gatling)

  // Spark ships its own Scala 2.13 stdlib and runtime; only the JDBC driver goes into the assembly
  val statDeps: Seq[ModuleID] = Seq(sparkSql % Provided, sparkKafka, postgres) ++ zioTest
}
