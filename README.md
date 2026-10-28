# map-event-pipeline

A demo event pipeline for road sensor events, built with **Scala 3**, **ZIO 2**, **Kafka / Redpanda**,
**PostgreSQL/PostGIS**, **Redis**, **Apache Spark** and **OpenTelemetry**.

Clients post events to an HTTP gateway, a stream processor deduplicates, enriches, escalates and routes them,
and Spark jobs aggregate per-region statistics that the gateway serves back: a batch job for the totals and a
Structured Streaming job for hourly counts, kept current from Kafka. Traces, metrics and logs from the gateway and the
processor go to Grafana over OpenTelemetry; one trace follows an event from the HTTP request through Kafka to Postgres.

## Architecture

```
 clients                        browsers: /map (Leaflet) ◀── server-sent events
    │  POST /api/v1/events            GET /api/v1/events/nearby, /recent, /live, /api/v1/stats
    ▼                                   ▲
┌──────────┐  map.events.raw   ┌──────────┐   map.events.enriched ──┐
│   gate   │ ────────────────▶ │   pipe   │ ─▶ map.events.{hazards,signs,traffic}
│ zio-http │ ◀─────────────────┼──────────┼─────────────────────────┘ (tailed for the live map)
└──────────┘                   └──────────┘
   │    ▲                         │    │
   │    │ map_events              │    └──────▶ PostgreSQL ◀──────┐
   │    └─────────────────────────┼──────────── map_events        │ map_event_stats
   │                              │                               │
   └── Redis: idempotency keys    └── Redis: dedup, region cache, ┌──────────┐
              region cache                   escalation counters  │   stat   │
                                                                  │  Spark   │
                                                                  └──────────┘
```

| Module | Role |
|--------|------|
| `core` | Domain model and JSON codecs, typed config, Postgres/PostGIS stores (Quill, Flyway, Chimney), Kafka producer, Redis caches, region resolution |
| `gate` | HTTP API (zio-http): ingest events with optional idempotency, query nearby/recent events and region stats, stream finished events to the live map (SSE) |
| `pipe` | Kafka consumer (zio-kafka): dedup → enrich with the oblast → escalate severity → persist → publish to typed topics |
| `stat` | Spark jobs: a batch job counts enriched events per region and type into `map_event_stats`; a Structured Streaming job (`TrendsApp`) keeps hourly counts per region and type in `map_event_trends` from `map.events.enriched` |
| `it`   | Integration tests against real Postgres, Redis and Kafka (Testcontainers) |

### Delivery guarantees

- **Gate.** With an `Idempotency-Key` header, retries of the same request map to the same event id and are
  published once (Redis `SET NX`, 24 h). Without the header every request is a new event.
- **Pipe.** At-least-once. Offsets are committed only after an event is handled. Transient failures are retried
  with backoff; if they persist, the stream stops without committing, so the record is redelivered after a restart.
  A malformed record is never dropped: it is published as-is to `map.events.dlq` with headers naming the error and
  its source topic, partition and offset, and only then committed. Writes are idempotent (`ON CONFLICT DO NOTHING`),
  and processed ids are remembered for 48 h under their own Redis keyspace.
- **Escalation.** A `Low` event becomes `Medium` once its region sees 5 events of the same type within 5 minutes.

### Topics

Brokers run with topic auto-creation **off**. Both apps run `TopicBootstrap` at startup (zio-kafka `AdminClient`):
it creates any missing topic with the partitions, replication factor and retention from `mep.kafka.topic-settings`
in `application.conf`, and tolerates gate and pipe racing to create the same topic.

| Topic | Written by | Contents | Retention |
|---|---|---|---|
| `map.events.raw` | gate | accepted events, JSON | 7 days |
| `map.events.enriched` | pipe | every processed event | 7 days |
| `map.events.{hazards,signs,traffic}` | pipe | processed events by type | 7 days |
| `map.events.dlq` | pipe | records that could not be decoded, payload unchanged; headers `mep.error`, `mep.source.topic`, `mep.source.partition`, `mep.source.offset`, `mep.failed-at` | 14 days |

All topics are keyed by event id (3 partitions by default; `KAFKA_REPLICATION_FACTOR` overrides the replication
factor for multi-broker clusters).

Existing topics are never altered: changing `topic-settings` only affects topics created afterwards. To change a
live topic, use `kafka-topics.sh --alter` / `kafka-configs.sh` (or `rpk topic alter-config`). Settings the broker can
never accept — `partitions = 0`, or a replication factor above the broker count — fail startup immediately with the
broker's message instead of being retried.

### Regions and geo queries

Events carry the **region** they happened in: the Ukrainian first-level region (oblast) that contains their
coordinates, e.g. `Kyiv City`, `Lviv Oblast`, `Autonomous Republic of Crimea`.

- **Nearby queries** use PostGIS. `map_events.geog` is a `geography` column generated from `lat`/`lon` (writers
  never touch it) with a GiST index, queried with `ST_DWithin` and ordered by `<->`: exact great-circle distance,
  correct near the poles and across the antimeridian, no candidate cap.
- **Enrichment** (`RegionResolver`): Redis cache (coordinates rounded to ~11 m, 6 h, key versioned) → the `regions` table
  (`ST_Covers`) → a 1° grid zone such as `zone_52_21` for points outside every region (other countries, open water,
  bays).
- **Data**: migration `V3__regions_ukraine.sql`, 27 regions, 267 KB. It comes from
  [Natural Earth](https://www.naturalearthdata.com/) 1:10m admin-1 (public domain), generated by
  `scala-cli run scripts/regions/buildSeed.sc`, which refuses to write the file unless the 27 regions are valid and
  their borders still line up after simplification. Natural Earth files Crimea and Sevastopol under another
  country, so regions are selected by ISO 3166-2 code (`UA-*`), not by country. Applied migrations never change;
  new data means a new migration.
- **Operations**: the schema needs the PostGIS extension (`V2` runs `CREATE EXTENSION postgis`), so use a PostGIS
  image (as compose does) or a managed Postgres with PostGIS available.
- **Upgrading** an existing deployment is in place, and was tested on a live stack: switch the image, restart.
  `V2`/`V3` add the column, index and regions; `V4` renames the grid zones (`zone_50_30`) that existing events
  carry to their region and drops the statistics rows that referred to them (re-run the `stat` job). Cached regions
  from the previous release are ignored, because the Redis key carries a version (`GeoCache.KeyVersion`).

## Tech stack

| | Version |
|---|---|
| Scala | 3.10.0 (`stat`: 3.7.4, see below) |
| JDK | 27 (`stat` runs on 21) |
| ZIO / zio-streams / zio-test | 2.1.27 |
| zio-http · zio-kafka · zio-redis | 3.11.6 · 3.8.1 · 1.3.0 |
| zio-json · zio-schema · zio-config · zio-logging | 1.1.1 · 1.8.7 · 4.1.0 · 2.5.3 |
| Quill (JDBC/ZIO) · Flyway · HikariCP | 4.8.6 · 13.11.1 · 7.1.0 |
| Chimney | 2.1.0 |
| zio-opentelemetry · OpenTelemetry SDK | 3.1.21 · 1.67.0 |
| grafana/otel-lgtm (collector, Tempo, Prometheus, Loki, Grafana) | 0.38.1, pinned by digest |
| Spark | 4.2.0 |
| Testcontainers | 2.0.5 |
| PostgreSQL + PostGIS · Redis | 18.6 + 3.6.4 (`postgis/postgis:18-3.6-alpine`, pinned by digest) · 8.6.3 |
| Apache Kafka (KRaft) · Redpanda | 4.3.0 · v26.1.9 |
| sbt · mill | 2.0.11 · 1.3.1 |

**Why `stat` is different:** Spark 4.x links against the Scala 2.13 standard library. From Scala 3.8 on, the standard
library is compiled by Scala 3, and Spark's runtime reflection fails on it. Spark also does not run on JDK 24+. So
`stat` is built with Scala 3.7.4, targets Java 21, does not depend on `core`, and uses only the untyped DataFrame API
(Dataset encoders need Scala 2 type tags).

## Quick start

Prerequisites: JDK 27, sbt **or** mill (`./mill` bootstraps itself), Docker with Compose v2.

```bash
cp .env.example .env        # set POSTGRES_PASSWORD; COMPOSE_PROFILES=kafka or redpanda
docker compose up -d --build
```

Try it:

```bash
curl -i -X POST localhost:8080/api/v1/events \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-1' \
  -d '{"eventType":"Hazard","severity":"Low","location":{"lat":50.4501,"lon":30.5234},"description":"pothole"}'

curl 'localhost:8080/api/v1/events/nearby?lat=50.45&lon=30.52&radius=5&limit=20'

docker compose run --rm stat              # aggregate into map_event_stats
curl 'localhost:8080/api/v1/stats?region=Kyiv%20City'

COMPOSE_PROFILES=kafka,stat docker compose up -d trends   # streaming job: hourly counts
curl 'localhost:8080/api/v1/stats/hourly?region=Kyiv%20City&hours=24'
```

`--profile stat` on the command line replaces `COMPOSE_PROFILES` from `.env` instead of adding to it, so the broker
would not start; list both profiles as above.

With the `redpanda` profile, the console is at `localhost:8081`.

Open the live map at `localhost:8080/map`, then feed it:

```bash
scripts/demo.sh             # 50 random events around Ukrainian cities, one every 0.5 s
```

### Observability (OpenTelemetry)

`docker compose up` also starts `grafana/otel-lgtm`: an OpenTelemetry collector with Tempo, Prometheus, Loki and
Grafana. Grafana is at `localhost:3000` with the **Map Event Pipeline** dashboard (traffic, latency, ingest and
processing rates, dead letters, live subscribers, recent traces, logs).

- **Traces.** The gate opens a server span per request that continues the caller's `traceparent`; each Kafka record
  carries the producer span's context in its headers, and the pipe continues it, with a span per step (dedup check,
  enrich, escalate, store, publish). So `POST /api/v1/events` and everything the pipe does for that event are one trace
  across both services; `PipelineSpec` checks this against real Kafka.
- **Metrics.** Plain ZIO metrics (`mep_http_server_*`, `mep_events_ingested`, `mep_events_processed`,
  `mep_records_dead_lettered`, `mep_event_processing_duration_ms`, `mep_live_subscribers`), plus ZIO's runtime and
  zio-kafka's own metrics, exported through zio-opentelemetry's bridge. Unknown paths are counted as `unmatched`, so a
  scanner cannot create unbounded labels.
- **Logs.** ZIO logs go to Loki with the `trace_id` and `span_id` of the fiber that wrote them (Grafana links log lines
  and traces both ways); the console output stays as it was.
- **No Java agent.** A spike with the OpenTelemetry agent on JDK 27 lost the caller's trace at the HTTP boundary: the
  agent keeps context in a thread-local, and ZIO moves fibers between threads. The SDK with zio-telemetry keeps the
  context in a `FiberRef` instead (400 concurrent requests, all kept their trace), and the few boundaries — HTTP in,
  Kafka out and in — are instrumented by hand.
- The SDK is configured by the standard `OTEL_*` variables (compose points it at `lgtm`). Apps run with sbt export to
  `localhost:4318`; without the container, set `OTEL_SDK_DISABLED=true`.

### Kubernetes (Helm)

`deploy/helm/map-event-pipeline` deploys the gate (Service, readiness and liveness on `/api/v1/health`) and the pipe,
with init containers that wait for Postgres and Kafka. `infra.enabled` (default) adds single-replica Postgres
(PostGIS), Redis and Kafka as StatefulSets for development and CI; turn it off and set `postgres.url`,
`redis.host` and `kafka.bootstrapServers` to use real services. `lgtm.enabled` adds grafana/otel-lgtm (about 1.2 GiB
more); without it the apps run with OpenTelemetry disabled. The Postgres password comes from `postgres.password` or
an existing Secret (`postgres.existingSecret`) — the chart has no default.

```bash
docker build --target gate -t map-event-gate:latest . && docker build --target pipe -t map-event-pipe:latest .
kind create cluster --name mep
kind load docker-image map-event-gate:latest map-event-pipe:latest --name mep
helm install mep deploy/helm/map-event-pipeline --set postgres.password=change-me --wait
scripts/k8s-smoke.sh mep          # posts an event and waits until the pipe has stored it
```

CI does the same in the `k8s` job on every push. The stack without lgtm used about 2.2 GiB in a kind cluster.

### Load testing (Gatling)

`load` is a Gatling 3.16 simulation, written with Gatling's Java DSL from Scala 3 (its Scala DSL is built for 2.13),
run against a running stack:

```bash
LOAD_PROFILE=baseline sbt -batch --server load/run   # smoke (default), baseline or stress; GATE_URL to point elsewhere
```

It mixes clients posting events (about one in ten retries a key, so duplicates take the 200 path), map clients
querying (`nearby`, `recent`, `hourly`) and following `/live` (each must receive an event), and a probe that posts an
event and polls `GET /api/v1/events/{id}` until it is stored — its group's duration is the end-to-end latency. The run
fails when more than 1% of requests fail or a p95 limit is exceeded; the HTML report lands in `target/gatling`. The
**Load test** workflow runs a profile on demand in CI and keeps the report.

Baseline profile (50 ingests/s, 20 queries/s, 10 live clients, 60 s) on a laptop (Docker Desktop, 4 CPUs, 3.8 GiB):

| | pipe processing one record at a time | partitions in parallel |
|---|---|---|
| HTTP errors | 0 | 0 |
| all requests p95 / p99 | 12.2 s / 17.1 s | **335 ms / 864 ms** |
| `POST /api/v1/events` p95 | 13.2 s | **546 ms** |
| ingest → stored p95 | 42 s | **5.1 s** |
| probes not stored within 60 s | 34 % | **0 %** |

- The first run (and the traces in Tempo) showed the pipe topping out at about 24 events/s: one sequential stream
  over all partitions, ~30 ms per event. Processing partitions in parallel, each in order, takes it to the ingest rate.
- The first probe polled `/recent?limit=200`; as latency grew, its polling became two thirds of all requests and loaded
  the gate it was measuring. It now looks the event up by id.
- Still open: the gate's producer waits longer for Kafka's acknowledgement than the pipe's on the same broker (p50
  59 ms vs 8 ms), so the baseline's own limits (POST p95 < 500 ms, end to end < 3 s) are not met yet.

### Hourly trends (Spark Structured Streaming)

`TrendsApp` (in `stat`, run by the `trends` compose service) reads `map.events.enriched` and keeps
`map_event_trends` (one row per hour, region and event type) up to date every 30 s; `GET /api/v1/stats/hourly`
serves it.

- Windows are by `createdAt`, the gate's receive time. Spark's watermark follows the newest event time seen, so
  windowing by the client's `occurredAt` would let one wrong clock mark every later event as late.
- Events more than 10 minutes behind the newest one seen are dropped as late (the batch job still counts them); the
  job logs how many each batch dropped, from Spark's progress report.
- The pipe may publish an event twice after a retry: duplicates are dropped by id within the watermark.
- Each micro-batch carries the full count of every window it changed (update mode) and is upserted, so a batch that
  Spark replays after a crash writes the same values again. Checked: forcing a replay restored a count edited by hand.
- The checkpoint (Kafka offsets and de-duplication state) lives in the `trends_checkpoint` volume. A fresh checkpoint
  starts from the oldest retained record, so the table is backfilled from the topic's retention (7 days) first. If
  the job was stopped for longer than the retention, it continues from the oldest record left (`failOnDataLoss`
  off) instead of failing on every restart; the counts for the expired hours are lost either way.
- Each batch's counts are written in one transaction, so `/stats/hourly` never shows half a batch.
- The Kafka source is not part of the `apache/spark` image, so `stat.jar` bundles it, without the jars the image
  already ships (Hadoop client, compression codecs, slf4j, Scala, `spark-tags`).
- The tests run the same query over text files with an in-memory sink (no Kafka, Postgres or Dataset encoders);
  the compose stack was checked against `map_events` hour by hour, before and after a restart.

### Live map

`/map` is a single page (Leaflet over OpenStreetMap tiles) that loads the latest events from `/api/v1/events/recent`
and then follows `/api/v1/events/live`, a stream of server-sent events. Markers are coloured by severity; the popup
shows type, region, description and time.

- Each gate tails `map.events.enriched` (what the pipe publishes after storing an event) with its own consumer group
  from the newest offset, so every gate instance sees every event and nothing is replayed or committed.
- One consumer per gate feeds a sliding `Hub`; each browser has its own subscription. A slow or stalled browser loses
  its oldest events instead of slowing Kafka or the other browsers (measured: five clients that never read, 4M events,
  flat heap). Delivery is best effort and the pipe may publish an event twice, so the page keys markers by id.
- On shutdown the gate ends the live streams before the HTTP server's graceful shutdown, which would otherwise wait
  its full timeout on the endless streams. Browsers reconnect by themselves (`EventSource`).
- Leaflet (pinned, with subresource integrity) and the map tiles load from the internet, like Swagger UI.
- The page reloads `/recent` whenever the stream (re)connects, so events published while the gate was unreachable
  show up after the reconnect.
- The gate does not limit the number of live connections (each holds a subscription with a 1024-event buffer). Put a
  reverse proxy with a connection limit in front of it before exposing it publicly.

### API

| Method | Path | Notes |
|---|---|---|
| `POST` | `/api/v1/events` | Body: `eventType` (`Hazard`/`Sign`/`Traffic`), `severity` (`Low`…`Critical`), `location {lat, lon}`, optional `description`, `occurredAt` (at most 5 minutes in the future: client clocks drift, but a future date would stay first in `/recent`). Optional `Idempotency-Key` header. `202` accepted, `200` duplicate, `400` invalid. |
| `GET` | `/api/v1/events/nearby` | `lat`, `lon` (required), `radius` km (default 5, max 100), `limit` (default 50, max 500). Exact great-circle distance, nearest first, plus the region of the query point. |
| `GET` | `/api/v1/events/{id}` | The event once the pipe has stored it; `404` before that. |
| `GET` | `/api/v1/events/recent` | `limit` (default 200, max 500). The latest events by occurrence time, newest first. |
| `GET` | `/api/v1/events/live` | Server-sent events (`event: event`, `id`, `data: <MapEvent JSON>`), one per event the pipe finishes, plus a `:heartbeat` comment every 15 s. Optional `bbox=minLon,minLat,maxLon,maxLat`. |
| `GET` | `/api/v1/stats` | Optional `region`. |
| `GET` | `/api/v1/stats/hourly` | Optional `region`, `hours` (default 24, max 168, the current hour included). Hourly counts from the streaming job, oldest first; hours without events are absent. |
| `GET` | `/api/v1/health` | Liveness. |
| `GET` | `/docs/openapi` | Swagger UI over the generated OpenAPI 3.1 document. |
| `GET` | `/map` | The live map. |

Every error body is `{"error": "..."}`, whatever the client's `Accept` header asks for: `400` for invalid input
(including unreadable JSON, an unknown enum value or a malformed query parameter) and `500` `{"error":"internal"}` for
failures on our side. Request bodies are read as JSON whatever `Content-Type` the client sends.

#### OpenAPI

The endpoints are declared once, as zio-http `Endpoint`s in [`GateApi`](gate/src/main/scala/com/github/olehbohatyi/mep/gate/GateApi.scala);
the routes are implemented from them and the OpenAPI document is generated from the same values, so the spec cannot
drift from the code. zio-json still writes the bytes (absent optional fields stay omitted); zio-schema only describes
them. [`OpenApiSpec`](gate/src/test/scala/com/github/olehbohatyi/mep/gate/OpenApiSpec.scala) compares the generated
document with the committed `gate/src/test/resources/openapi.json`, so API changes show up in review. After an intended
change run `UPDATE_OPENAPI=1 sbt -batch --server "gate/testOnly *OpenApiSpec"` and commit the file.

Swagger UI loads its JavaScript and CSS from `unpkg.com`, so the docs page needs internet access in the browser (the API
itself does not).

## Build and test

### sbt

```bash
sbt compile
sbt testFull                # unit tests; no Docker needed (`sbt test` is cached and skips unchanged suites)
sbt itTest                  # integration tests; needs Docker
sbt check                   # formatting + compile + unit tests (what CI runs)
sbt fmt                     # format everything
sbt gate/run                # needs the infrastructure from docker compose
```

The build runs on sbt 2. A plain `sbt` command is handed to a background sbt server, which keeps the environment it
was started with; when an environment variable matters (`SPARK_JAVA_HOME`, `UPDATE_OPENAPI`), run
`sbt -batch --server …` so the build runs in a fresh JVM with your current environment. `scripts/verify.sh` and CI do
that.

`stat`'s tests and `stat/run` fork the JDK at `SPARK_JAVA_HOME` (a Java 21). Without it they use the current JDK,
and Spark fails on 24+:

```bash
export SPARK_JAVA_HOME="$(cs java-home --jvm temurin:21)"
```

### mill

```bash
./mill __.compile
./mill core.test + gate.test + pipe.test + stat.test
./mill it.test              # needs Docker
./mill stat.assembly        # jar for spark-submit
```

mill fetches the Java 21 for `stat` by itself (`jvmId`). Versions live in both `project/Dependencies.scala` and
`build.mill`; CI builds with both.

### Everything CI runs

```bash
scripts/verify.sh           # formatting, compile, unit + integration tests, mill
scripts/verify.sh --quick   # formatting, compile, unit tests
```

## Built with Claude Code

This project is developed together with [Claude Code](https://claude.com/claude-code). The workflow is part of the
repository, so it is reproducible rather than anecdotal:

- **[`CLAUDE.md`](CLAUDE.md)** — the project brief Claude reads at the start of every session: commands, module
  layout, conventions and the non-obvious constraints (Spark's Scala/JDK limits, Redis keyspaces, delivery semantics).
- **`/verify`** ([`.claude/skills/verify`](.claude/skills/verify/SKILL.md)) — runs [`scripts/verify.sh`](scripts/verify.sh),
  the same checks as CI, and reports failures with file:line references. Changes are verified before they are
  committed, not after CI fails.
- **scalafmt hook** ([`.claude/hooks/scalafmt.sh`](.claude/hooks/scalafmt.sh)) — formats every Scala file Claude edits
  and feeds parse errors straight back, so formatting never reaches review.

How the work is organised:

- Each phase starts with a short spike that proves the risky parts (for example, that Spark 4.2 cannot load the
  Scala 3.8+ standard library, which is why `stat` is pinned to Scala 3.7.4) before any code is written.
- Features land as small Conventional Commits; every bug fix carries a regression test.
- Each phase is a pull request with green CI before merge.

## Project layout

```
├── build.sbt, project/       sbt build (versions in project/Dependencies.scala)
├── build.mill, mill          mill build + launcher
├── Dockerfile                build stage + gate / pipe / stat targets
├── docker-compose.yml        infrastructure, apps, `stat` profile (batch `stat`, streaming `trends`)
├── deploy/helm/              Helm chart (gate, pipe, optional in-cluster infra and otel-lgtm)
├── observability/            Grafana dashboard and its provisioning (mounted into otel-lgtm)
├── scripts/verify.sh         the checks CI runs
├── scripts/regions/          buildSeed.sc: regenerates the region polygons from Natural Earth
├── CLAUDE.md, .claude/       Claude Code brief, /verify skill, scalafmt hook
├── core/src/main
│   ├── resources/            application.conf, logback.xml, db/migration (Flyway)
│   └── scala/…/mep/
│       ├── domain/           MapEvent, GeoPoint, enums
│       ├── config/           AppConfig and per-component configs
│       ├── stores/           Quill context, EventRepo, StatsRepo, Migrations, DataSource
│       ├── broker/           ProducerService, EventCodec
│       ├── caches/           Redis, IdempotencyCache, GeoCache
│       ├── geo/              RegionResolver
│       └── telemetry/        Telemetry: OpenTelemetry SDK, fiber context, logging bridge
├── gate/                     GateRoutes, IngestionService, QueryService, GateApp
├── pipe/                     StreamProcessor, EventHandler, Dedup/Enrichment/Escalation, PipeApp
├── stat/                     EventAggregator, StatApp (batch); Trends, TrendsApp (streaming)
├── load/                     Gatling simulation (Java DSL from Scala 3) and its profiles
└── it/                       Testcontainers specs
```
