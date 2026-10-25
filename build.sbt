import Dependencies.*

ThisBuild / organization := "com.github.olehbohatyi"
ThisBuild / scalaVersion := Versions.scala
ThisBuild / version      := "0.1.0-SNAPSHOT"

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Werror",
  "-Wunused:all",
  "-Wvalue-discard",
  "-Wnonunit-statement",
)

// Scala's LazyVals still touches sun.misc.Unsafe; silence the JDK 24+ deprecation banner
lazy val jvmFlags = Seq("--sun-misc-unsafe-memory-access=allow")

def forkSettings(flags: Seq[String]) = Seq(
  Test / fork := true,
  Test / javaOptions ++= flags,
  run / fork  := true,
  run / javaOptions ++= flags,
)

lazy val commonSettings = forkSettings(jvmFlags) :+ (Compile / doc / sources := Nil)

// Spark 4.x does not run on JDK 24+ (it needs jdk.internal.ref.Cleaner), so stat's
// tests and `run` fork the JDK 21 at SPARK_JAVA_HOME when it is set
lazy val sparkJavaHome = sys.env.get("SPARK_JAVA_HOME").map(file)

lazy val sparkOpens = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
)

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    name := "core",
    libraryDependencies ++= coreDeps,
  )

lazy val gate = (project in file("gate"))
  .dependsOn(core % "compile->compile;test->test")
  .enablePlugins(JavaAppPackaging)
  .settings(
    commonSettings,
    name                := "gate",
    libraryDependencies ++= gateDeps,
    Compile / mainClass := Some("com.github.olehbohatyi.mep.gate.GateApp"),
    Universal / javaOptions ++= jvmFlags.map("-J" + _),
  )

lazy val pipe = (project in file("pipe"))
  .dependsOn(core % "compile->compile;test->test")
  .enablePlugins(JavaAppPackaging)
  .settings(
    commonSettings,
    name                := "pipe",
    libraryDependencies ++= pipeDeps,
    Compile / mainClass := Some("com.github.olehbohatyi.mep.pipe.PipeApp"),
    Universal / javaOptions ++= jvmFlags.map("-J" + _),
  )

// Spark 4.x links against the Scala 2.13 stdlib, so stat stays on Scala 3.7 and
// does not depend on core (which is on 3.10). It runs via spark-submit on Java 21.
lazy val stat = (project in file("stat"))
  .settings(
    forkSettings(sparkOpens),
    Compile / doc / sources          := Nil,
    Test / javaHome                  := sparkJavaHome,
    run / javaHome                   := sparkJavaHome,
    name                             := "stat",
    scalaVersion                     := Versions.scalaSpark,
    scalacOptions ++= Seq("-release", "21"),
    libraryDependencies ++= statDeps,
    Compile / mainClass              := Some("com.github.olehbohatyi.mep.stat.StatApp"),
    // `sbt stat/run` needs the Provided Spark jars on the classpath
    Compile / run                    := Defaults
      .runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner)
      .evaluated,
    assembly / mainClass             := Some("com.github.olehbohatyi.mep.stat.StatApp"),
    assembly / assemblyOutputPath    := target.value / "stat.jar",
    // Spark provides scala-library 2.13; keep only scala3-library in the fat jar
    assembly / assemblyExcludedJars  := (assembly / fullClasspath)
      .value
      .filter(_.data.getName.startsWith("scala-library")),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "versions", _, "module-info.class") => MergeStrategy.discard
      case "module-info.class"                                      => MergeStrategy.discard
      case x                                                        => (assembly / assemblyMergeStrategy).value(x)
    },
  )

// Integration tests against real Postgres, Redis and Kafka (Testcontainers; needs Docker)
lazy val it = (project in file("it"))
  .dependsOn(core % "compile->compile;test->test", gate, pipe)
  .settings(
    commonSettings,
    name                     := "it",
    publish / skip           := true,
    libraryDependencies ++= itDeps,
    Test / parallelExecution := false,
  )

// `it` is left out of the aggregate so `sbt test` runs without Docker; see the aliases
lazy val root = (project in file("."))
  .aggregate(core, gate, pipe, stat)
  .settings(
    name           := "map-event-pipeline",
    publish / skip := true,
  )

addCommandAlias("fmt", "scalafmtSbt; scalafmtAll; it/scalafmtAll")
addCommandAlias("check", "scalafmtSbtCheck; scalafmtCheckAll; it/scalafmtCheckAll; Test/compile; it/Test/compile; test")
addCommandAlias("itTest", "it/test")
