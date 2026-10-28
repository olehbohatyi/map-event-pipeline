package com.github.olehbohatyi.mep.load

import java.time.Duration

/** How hard and how long to push, and what has to hold. Rates are new users per second in Gatling's open model: every
  * ingest or query user sends one request.
  */
final case class LoadProfile(
    name: String,
    rampUp: Duration,
    duration: Duration,
    ingestPerSec: Double,
    queriesPerSec: Double,
    liveClients: Int,
    probesPerSec: Double,
    ingestP95Ms: Int,
    endToEndP95Ms: Int,
  )

object LoadProfile:

  // A quick check that everything answers; also what CI runs
  val Smoke = LoadProfile("smoke", Duration.ofSeconds(5), Duration.ofSeconds(20), 5, 2, 2, 0.5, 1000, 10000)

  // Steady traffic: the numbers in the README come from this one
  val Baseline = LoadProfile("baseline", Duration.ofSeconds(10), Duration.ofSeconds(60), 50, 20, 10, 1, 500, 3000)

  // Well past what a laptop stack is sized for, to see where it bends
  val Stress = LoadProfile("stress", Duration.ofSeconds(30), Duration.ofSeconds(90), 200, 60, 25, 1, 1000, 10000)

  val all: List[LoadProfile] = List(Smoke, Baseline, Stress)

  def fromEnv(env: Map[String, String]): LoadProfile =
    val name = env.getOrElse("LOAD_PROFILE", Smoke.name)
    all
      .find(_.name == name)
      .getOrElse(throw IllegalArgumentException(s"LOAD_PROFILE must be one of ${all.map(_.name).mkString(", ")}"))
