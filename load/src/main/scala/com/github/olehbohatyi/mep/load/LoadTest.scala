package com.github.olehbohatyi.mep.load

/** Runs LoadSimulation and writes Gatling's HTML report under LOAD_RESULTS (default target/gatling). Gatling exits with
  * a non-zero code when an assertion fails, so CI fails with it.
  *
  * LOAD_PROFILE=baseline sbt -batch --server load/run
  */
object LoadTest:
  def main(args: Array[String]): Unit =
    val results = sys.env.getOrElse("LOAD_RESULTS", "target/gatling")
    io.gatling.app.Gatling.main(Array("-s", classOf[LoadSimulation].getName, "-rf", results))
