# map-event-pipeline

Road-event pipeline: `gate` (zio-http API) → Kafka/Redpanda → `pipe` (zio-kafka processor) → PostgreSQL, with Redis
for idempotency/dedup/caches and a Spark job (`stat`) aggregating per-region stats. README.md has the architecture,
API and delivery guarantees.

## Commands

```bash
scripts/verify.sh --quick   # what to run while iterating: scalafmt check, compile, unit tests
scripts/verify.sh           # everything CI runs: + Testcontainers integration tests + mill build (/verify)
sbt fmt                     # format everything (never hand-format)
sbt -batch --server "gate/testOnly *GateRoutesSpec"   # one spec
sbt itTest                  # integration tests only; needs Docker
LOAD_PROFILE=smoke sbt -batch --server load/run   # Gatling against a running stack (not in the aggregate)
docker compose up -d --build          # full stack (cp .env.example .env first)
```

## Layout

- `core` — domain, config (HOCON → zio-config), Quill stores + Flyway migrations (PostGIS), Kafka producer, Redis
  caches, region resolver. Shared test doubles live in `core/src/test/.../testkit` and are reused by gate/pipe tests.
- `gate`, `pipe` — apps; each `*App` wires layers with `.provide(...)`.
- `stat` — Spark job. **Scala 3.7.4, Java 21, does not depend on `core`** (see Gotchas).
- `it` — Testcontainers specs (Postgres, Redis, Kafka); excluded from `sbt test`.

## Conventions

- Scala 3 indentation syntax, ZIO 2 service pattern: `trait X` + `object X { val live: ZLayer[...] }` +
  `final private class XLive`. Typed config objects, no `sys.env` outside `stat`.
- Compiler runs with `-Werror -Wunused:all -Wvalue-discard -Wnonunit-statement`: no unused imports/params,
  discard values explicitly (`.unit`, `: Unit`).
- Every bug fix comes with a regression test; integration behaviour goes in `it/`, logic in unit specs with the
  `testkit` fakes.
- Commits: single-line Conventional Commits (`feat(gate): …`, `fix(pipe): …`), no body, no co-author trailer.
  One branch + PR per phase; CI must be green before merge.
- Versions live in **both** `project/Dependencies.scala` and `build.mill`; change them together.

## Gotchas

- sbt 2: run it as `sbt -batch --server …` so it uses this shell's environment; plain `sbt` hands the commands to a
  background server that keeps the environment it was started with (a server started without `SPARK_JAVA_HOME` runs
  the Spark tests on JDK 27 and they fail). `test` is cached and skips unchanged suites, so the aliases use
  `testFull`. Several commands go in one `;`-separated argument (`"gate/stage; pipe/stage"`). Output lives under
  `target/out/jvm/scala-<version>/<project>/` (the Dockerfile copies from there). Write settings out per project: a
  shared helper taking parameters is evaluated once, so a second project silently gets the first one's values.

- Spark 4.x links against the Scala 2.13 stdlib and cannot run on Scala ≥ 3.8 or JDK ≥ 24. `stat` stays on
  Scala 3.7.4 (line marked `scala-steward:off`), compiles with `-release 21`, uses only the DataFrame API
  (no `spark.implicits` / encoders), and its tests fork the JDK in `SPARK_JAVA_HOME`
  (`cs java-home --jvm temurin:21`; `scripts/verify.sh` sets it). mill picks Java 21 itself via `jvmId`.
- Streaming (`stat/Trends`, `TrendsApp`): windows by `createdAt` (server time), never `occurredAt` — the watermark
  follows the max event time, so a client clock could make every later event late. The sink upserts absolute counts
  (update mode) so replayed batches are harmless; window starts go into `timestamptz` (a plain `timestamp` column
  would shift by the JVM's zone). The Kafka connector is bundled in `stat.jar` minus what the Spark image ships —
  keep the exclusions in both builds in step. Test streaming via `readStream.text` + `foreachBatch` (the memory sink
  cannot restart from a checkpoint, `MemoryStream` needs encoders); share `TestSpark.layer`, never stop the session.
- Telemetry (`core/telemetry/Telemetry`): no Java agent — trace context lives in a `FiberRef` (zio-telemetry
  `contextZIO`). Apps are `ZIOApp`s whose `bootstrap` builds the SDK, the one context store and the logging bridge;
  build `Telemetry.tracing`/`metrics` on top of that `Env`, never a second `contextZIO` (logs would lose trace ids).
  HTTP in (`GateRoutes.observed`), Kafka out (`ProducerService`) and in (`TraceHeaders.continue`) are instrumented by
  hand. zio-http's server merges the app with `++`, which keeps the *left* not-found handler, so unknown paths get a
  real catch-all route to be traced. Tests use `testkit.TestTelemetry` (`tracing` no-op, `recording` in-memory spans).
  Metric labels must stay low-cardinality.
- Load test (`load`): Gatling's Java DSL from Scala 3 — `then` needs backticks, import only `Simulation` from
  `io.gatling.javaapi.core` (its package also has an `exec`), and the forked JVM needs
  `--add-opens=java.base/java.lang=ALL-UNNAMED`. A probe must not add load that grows with latency (it polls the
  event by id). The pipe processes partitions in parallel, in order within each; keep it that way.
- Helm chart (`deploy/helm/map-event-pipeline`): Kafka's advertised listener must be its own Service name
  (`<release>-kafka:9092`); apps wait for Postgres/Kafka in init containers; images are loaded into kind
  (`pullPolicy: Never`). The `k8s` CI job installs it on kind and runs `scripts/k8s-smoke.sh`.
- Compose: `--profile stat` replaces `COMPOSE_PROFILES` from `.env` (no broker then); use
  `COMPOSE_PROFILES=kafka,stat docker compose …`.
- Quill 4.8 pulls zio-json 0.7; it is excluded in both builds because zio-json 1.x is used. Import Quill's
  `SnakeCase` explicitly — zio-json 1.x exports one too.
- Gate idempotency keys (`mep:gate:idem:*`) and pipe processed ids (`mep:pipe:processed:*`) must stay in separate
  Redis keyspaces.
- The pipe commits Kafka offsets only after an event is handled; persistent failures stop the stream instead of
  skipping, malformed records go to the `map.events.dlq` dead-letter topic (`RecordHandler`) before their offset is
  committed. Keep inserts idempotent (`onConflictIgnore`).
- Broker auto-create is off (compose and the Testcontainers Kafka). New topics must be added to `TopicsConfig` so
  `TopicBootstrap` creates them; tests build Kafka config with `testkit.TestKafka.config`.
- PostGIS: `map_events.geog` is a generated column — never write it, and `MapEventRow` deliberately does not map it.
  Spatial predicates go through Quill's `sql"…"` with `lift` (see `EventRepo.findNearby`, `RegionRepo`); use `_`
  for lambda parameters the SQL does not reference (`-Wunused:all`). Flyway migrations are immutable: the region
  seed `V3` is generated by `scripts/regions/buildSeed.sc` (Natural Earth, selected by ISO code `UA-*` because it
  files Crimea and Sevastopol under another country); changed data means a new `V<n>`. Postgres in tests and compose
  is the `postgis/postgis:18-3.6-alpine` image, pinned by digest in `docker-compose.yml` and `it/.../Containers.scala`
  (upstream has no patch tag; update both together). Testcontainers needs `asCompatibleSubstituteFor("postgres")`.
- Redis outlives deployments: change how regions are resolved or named and cached values from the old code keep
  being served until their TTL. Bump `GeoCache.KeyVersion` (and add a data migration like `V4` if stored values
  change). Test upgrades with real state — a unit test cannot see a stale Redis or an old schema.
- The gate API is declared as zio-http `Endpoint`s in `GateApi`; routes and the OpenAPI document both come from them.
  zio-json writes the bytes (wire format: enums as plain strings, `None` omitted), zio-schema (`derives Schema`, enums
  need `@simpleEnum`) only feeds the spec. Changing the API means regenerating the golden file:
  `UPDATE_OPENAPI=1 sbt -batch --server "gate/testOnly *OpenApiSpec"`. zio-http's own decode errors are encoded as
  `{"error": ...}` (`GateApi.invalidRequest` replaces its Accept-negotiated HTML/JSON default; every endpoint needs
  `.invalidRequestAsJson`, a test checks it), and request bodies are read as JSON whatever `Content-Type` says
  (`GateRoutes.readBodyAsJson`), as before the Endpoint API.
- Live map: `LiveFeed` tails `map.events.enriched` with a random consumer group per gate instance (newest offset, no
  commits) into a sliding `Hub`; never let a subscriber block the source. `GateApp` calls `feed.close` before the
  server's graceful shutdown, or open SSE streams hold shutdown for its full timeout. zio-http's OpenAPI generator
  omits the `ServerSentEvent` schema it refers to; `GateApi.openApi` adds it and `OpenApiSpec` checks every `$ref`.
  Tests that read an SSE body subscribe first (`feed.subscribers`) and then publish; the map page builds popups with
  `textContent` because `description` is user input.
- Inside compose the apps reach Kafka as `broker:9092` (network alias of both broker profiles); from the host it is
  `localhost:${KAFKA_PORT}`.

## Claude Code setup in this repo

- `/verify` (`.claude/skills/verify`) runs `scripts/verify.sh` and reports failures with file:line links.
- A PostToolUse hook (`.claude/hooks/scalafmt.sh`) formats each `.scala`/`.sbt` file after Edit/Write and reports
  parse errors back. It uses the first `scalafmt` on `PATH`, else `cs launch scalafmt`. The JVM CLI (incl.
  `cs install scalafmt`) takes ~4 s per edit; the GraalVM native binary from the scalafmt GitHub release
  (`scalafmt-<arch>-apple-darwin.zip`, or the `linux` assets) is near-instant.
