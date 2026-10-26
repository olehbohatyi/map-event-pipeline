# map-event-pipeline

Road-event pipeline: `gate` (zio-http API) → Kafka/Redpanda → `pipe` (zio-kafka processor) → PostgreSQL, with Redis
for idempotency/dedup/caches and a Spark job (`stat`) aggregating per-region stats. README.md has the architecture,
API and delivery guarantees.

## Commands

```bash
scripts/verify.sh --quick   # what to run while iterating: scalafmt check, compile, unit tests
scripts/verify.sh           # everything CI runs: + Testcontainers integration tests + mill build (/verify)
sbt fmt                     # format everything (never hand-format)
sbt "gate/testOnly *GateRoutesSpec"   # one spec
sbt itTest                  # integration tests only; needs Docker
docker compose up -d --build          # full stack (cp .env.example .env first)
```

## Layout

- `core` — domain, config (HOCON → zio-config), Quill stores + Flyway migrations, Kafka producer, Redis caches,
  region resolver. Shared test doubles live in `core/src/test/.../testkit` and are reused by gate/pipe tests.
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

- Spark 4.x links against the Scala 2.13 stdlib and cannot run on Scala ≥ 3.8 or JDK ≥ 24. `stat` stays on
  Scala 3.7.4 (line marked `scala-steward:off`), compiles with `-release 21`, uses only the DataFrame API
  (no `spark.implicits` / encoders), and its tests fork the JDK in `SPARK_JAVA_HOME`
  (`cs java-home --jvm temurin:21`; `scripts/verify.sh` sets it). mill picks Java 21 itself via `jvmId`.
- Quill 4.8 pulls zio-json 0.7; it is excluded in both builds because zio-json 1.x is used. Import Quill's
  `SnakeCase` explicitly — zio-json 1.x exports one too.
- Gate idempotency keys (`mep:gate:idem:*`) and pipe processed ids (`mep:pipe:processed:*`) must stay in separate
  Redis keyspaces.
- The pipe commits Kafka offsets only after an event is handled; persistent failures stop the stream instead of
  skipping, malformed records go to the `map.events.dlq` dead-letter topic (`RecordHandler`) before their offset is
  committed. Keep inserts idempotent (`onConflictIgnore`).
- Broker auto-create is off (compose and the Testcontainers Kafka). New topics must be added to `TopicsConfig` so
  `TopicBootstrap` creates them; tests build Kafka config with `testkit.TestKafka.config`.
- Inside compose the apps reach Kafka as `broker:9092` (network alias of both broker profiles); from the host it is
  `localhost:${KAFKA_PORT}`.

## Claude Code setup in this repo

- `/verify` (`.claude/skills/verify`) runs `scripts/verify.sh` and reports failures with file:line links.
- A PostToolUse hook (`.claude/hooks/scalafmt.sh`) formats each `.scala`/`.sbt` file after Edit/Write and reports
  parse errors back. It uses the first `scalafmt` on `PATH`, else `cs launch scalafmt`. The JVM CLI (incl.
  `cs install scalafmt`) takes ~4 s per edit; the GraalVM native binary from the scalafmt GitHub release
  (`scalafmt-<arch>-apple-darwin.zip`, or the `linux` assets) is near-instant.
