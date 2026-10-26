# map-event-pipeline

A demo event pipeline for road sensor events, built with **Scala 3**, **ZIO 2**, **Kafka / Redpanda**,
**PostgreSQL**, **Redis** and **Apache Spark**.

Clients post events to an HTTP gateway, a stream processor deduplicates, enriches, escalates and routes them,
and a Spark batch job aggregates per-region statistics that the gateway serves back.

## Architecture

```
 clients
    │  POST /api/v1/events            GET /api/v1/events/nearby, /api/v1/stats
    ▼                                   ▲
┌──────────┐  map.events.raw   ┌──────────┐   map.events.enriched
│   gate   │ ────────────────▶ │   pipe   │ ─▶ map.events.{hazards,signs,traffic}
│ zio-http │                   │zio-kafka │
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
| `core` | Domain model and JSON codecs, typed config, Postgres stores (Quill, Flyway, Chimney), Kafka producer, Redis caches, region resolution |
| `gate` | HTTP API (zio-http): ingest events with optional idempotency, query nearby events and region stats |
| `pipe` | Kafka consumer (zio-kafka): dedup → enrich region → escalate severity → persist → publish to typed topics |
| `stat` | Spark batch job: counts enriched events per region and type into `map_event_stats` |
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

## Tech stack

| | Version |
|---|---|
| Scala | 3.10.0 (`stat`: 3.7.4, see below) |
| JDK | 27 (`stat` runs on 21) |
| ZIO / zio-streams / zio-test | 2.1.27 |
| zio-http · zio-kafka · zio-redis | 3.11.6 · 3.8.1 · 1.3.0 |
| zio-json · zio-config · zio-logging | 1.1.1 · 4.1.0 · 2.5.3 |
| Quill (JDBC/ZIO) · Flyway · HikariCP | 4.8.6 · 13.11.1 · 7.1.0 |
| Chimney | 2.1.0 |
| Spark | 4.2.0 |
| Testcontainers | 2.0.5 |
| PostgreSQL · Redis | 18.4 · 8.6.3 |
| Apache Kafka (KRaft) · Redpanda | 4.3.0 · v26.1.9 |
| sbt · mill | 1.13.0 · 1.3.1 |

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
curl 'localhost:8080/api/v1/stats?region=zone_50_30'
```

With the `redpanda` profile, the console is at `localhost:8081`.

### API

| Method | Path | Notes |
|---|---|---|
| `POST` | `/api/v1/events` | Body: `eventType` (`Hazard`/`Sign`/`Traffic`), `severity` (`Low`…`Critical`), `location {lat, lon}`, optional `description`, `occurredAt`. Optional `Idempotency-Key` header. `202` accepted, `200` duplicate, `400` invalid. |
| `GET` | `/api/v1/events/nearby` | `lat`, `lon` (required), `radius` km (default 5, max 100), `limit` (default 50, max 500). Nearest first, plus the region of the query point. |
| `GET` | `/api/v1/stats` | Optional `region`. |
| `GET` | `/api/v1/health` | Liveness. |

## Build and test

### sbt

```bash
sbt compile
sbt test                    # unit tests; no Docker needed
sbt itTest                  # integration tests; needs Docker
sbt check                   # formatting + compile + unit tests (what CI runs)
sbt fmt                     # format everything
sbt gate/run                # needs the infrastructure from docker compose
```

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
├── docker-compose.yml        infrastructure, apps, `stat` profile
├── scripts/verify.sh         the checks CI runs
├── CLAUDE.md, .claude/       Claude Code brief, /verify skill, scalafmt hook
├── core/src/main
│   ├── resources/            application.conf, logback.xml, db/migration (Flyway)
│   └── scala/…/mep/
│       ├── domain/           MapEvent, GeoPoint, enums
│       ├── config/           AppConfig and per-component configs
│       ├── stores/           Quill context, EventRepo, StatsRepo, Migrations, DataSource
│       ├── broker/           ProducerService, EventCodec
│       ├── caches/           Redis, IdempotencyCache, GeoCache
│       └── geo/              RegionResolver
├── gate/                     GateRoutes, IngestionService, QueryService, GateApp
├── pipe/                     StreamProcessor, EventHandler, Dedup/Enrichment/Escalation, PipeApp
├── stat/                     EventAggregator, StatApp
└── it/                       Testcontainers specs
```
