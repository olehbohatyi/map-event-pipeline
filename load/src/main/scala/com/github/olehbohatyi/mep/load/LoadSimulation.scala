package com.github.olehbohatyi.mep.load

import java.time.Duration
import java.util.UUID

import io.gatling.javaapi.core.CoreDsl.*
import io.gatling.javaapi.core.Simulation
import io.gatling.javaapi.http.HttpDsl.*

/** Traffic against a running stack (GATE_URL, default http://localhost:8080): clients posting events, map clients
  * querying and following the live stream, and a probe timing how long an event takes from POST to stored. Written with
  * Gatling's Java DSL, which works from Scala 3 (its Scala DSL is built for 2.13).
  */
final class LoadSimulation extends Simulation:

  private val profile = LoadProfile.fromEnv(sys.env)
  private val gateUrl = sys.env.getOrElse("GATE_URL", "http://localhost:8080")

  private val protocol = http.baseUrl(gateUrl).acceptHeader("application/json").contentTypeHeader("application/json")

  private val ingest = scenario("ingest")
    .feed(Events.feeder)
    .exec(
      http("POST /api/v1/events")
        .post("/api/v1/events")
        .header("Idempotency-Key", "#{key}")
        .body(StringBody("#{body}"))
        // 202 for a new event, 200 when the key was seen before
        .check(status().in(200, 202))
    )

  // `then` is a keyword in Scala 3, hence the backticks on the Java DSL method
  private val queries = scenario("queries")
    .feed(Events.feeder)
    .randomSwitch()
    .on(
      percent(50.0).`then`(
        exec(
          http("GET /api/v1/events/nearby")
            .get("/api/v1/events/nearby?lat=#{lat}&lon=#{lon}&radius=25")
            .check(status().is(200))
        )
      ),
      percent(25.0).`then`(
        exec(http("GET /api/v1/events/recent").get("/api/v1/events/recent?limit=100").check(status().is(200)))
      ),
      percent(25.0).`then`(
        exec(http("GET /api/v1/stats/hourly").get("/api/v1/stats/hourly?hours=6").check(status().is(200)))
      ),
    )

  // A map client: the stream must deliver an event; then it stays connected for the rest of the run
  private val live = scenario("live map")
    .exec(
      sse("live connect")
        .get("/api/v1/events/live")
        .await(Duration.ofSeconds(30))
        .on(sse.checkMessage("event").check(regex("\"id\"")))
    )
    .pause(profile.duration)
    .exec(sse("live close").close())

  /** POST an event, then look it up by id until the pipe has stored it; the group's duration is the end-to-end latency.
    * A primary-key read on purpose: the first version polled /recent?limit=200, and as latency grew the polling made up
    * two thirds of all requests and loaded the gate it was measuring.
    */
  private val PollEvery = Duration.ofMillis(250)
  private val MaxPolls  = 240 // a minute
  private val probe     = scenario("end-to-end probe")
    .feed(Events.feeder)
    .group("ingest to stored")
    .on(
      exec(
        http("probe POST")
          .post("/api/v1/events")
          .header("Idempotency-Key", _ => s"probe-${UUID.randomUUID()}")
          .body(StringBody("#{body}"))
          .check(status().is(202), jsonPath("$.id").saveAs("probeId"))
      ).exec(_.set("stored", false))
        .asLongAs(s => !s.getBoolean("stored") && s.getInt("polls") < MaxPolls, "polls")
        .on(
          pause(PollEvery)
            .exec(
              http("probe GET /api/v1/events/{id}")
                .get("/api/v1/events/#{probeId}")
                // 404 until the pipe has stored it
                .check(status().in(200, 404).saveAs("probeStatus"))
            )
            .exec(s => s.set("stored", s.getInt("probeStatus") == 200))
        )
        // Not stored within a minute: the group fails
        .exec(s => if s.getBoolean("stored") then s else s.markAsFailed())
    )

  private val total = profile.rampUp.plus(profile.duration)

  private val _ =
    setUp(
      ingest.injectOpen(
        rampUsersPerSec(1).to(profile.ingestPerSec).during(profile.rampUp),
        constantUsersPerSec(profile.ingestPerSec).during(profile.duration),
      ),
      queries.injectOpen(
        rampUsersPerSec(1).to(profile.queriesPerSec).during(profile.rampUp),
        constantUsersPerSec(profile.queriesPerSec).during(profile.duration),
      ),
      live.injectOpen(rampUsers(profile.liveClients).during(profile.rampUp)),
      probe.injectOpen(constantUsersPerSec(profile.probesPerSec).during(total)),
    ).protocols(protocol)
      .assertions(
        global().failedRequests().percent().lt(1.0),
        details("POST /api/v1/events").responseTime().percentile(95.0).lt(profile.ingestP95Ms),
        details("ingest to stored").responseTime().percentile(95.0).lt(profile.endToEndP95Ms),
        details("ingest to stored").failedRequests().percent().lt(1.0),
      )
