# Decisions and findings

Each phase started with a short spike that tried the risky parts before any code was written. This is what those
spikes and the work after them decided, with the evidence. Pull requests are linked by number.

| Area | Decision | Why (measured or reproduced) |
|---|---|---|
| Spark on Scala 3 | `stat` stays on Scala 3.7.4 and Java 21, DataFrame API only | Spark 4.x links the Scala 2.13 library; from Scala 3.8 the library is compiled by Scala 3 and Spark's reflection fails, and Spark does not run on JDK 24+. Dataset encoders need Scala 2 type tags. |
| Kafka delivery (#3) | Topics created by the apps (`TopicBootstrap`), broker auto-create off; offsets committed only after an event is handled; unreadable records go to a dead-letter topic | A persistent failure stops the stream instead of skipping records; a poison record cannot block a partition. |
| Regions (#5) | PostGIS polygons of Ukraine's oblasts (Natural Earth, selected by ISO code), `ST_Covers` / `ST_DWithin` on a generated geography column | Replaced a 1° grid. An in-place upgrade test with real state found cached regions from the old code still being served — fixed with versioned cache keys and a backfill migration. |
| OpenAPI (#7, #9) | zio-http `Endpoint`s as the single source for routes and the document; zio-json still writes the bytes | Keeps the existing wire format (absent fields omitted). zio-http rendered its own decode errors as HTML for browsers and rejected non-JSON content types: both fixed with regression tests. |
| sbt 2 (#8) | Run as `sbt -batch --server` | A background sbt server keeps the environment it started with: reproduced the Spark tests running on the wrong JDK. Parameterised settings helpers are evaluated once, `test` is cached. |
| Live map (#10) | Server-sent events from a sliding `Hub` fed by one Kafka consumer per gate | Five stalled clients and 4M events: flat heap. Open streams held server shutdown for 12.4 s until the feed is closed first (2.1 s). A future-dated event (#11) would have stayed on the map for good. |
| Streaming trends (#12) | Spark Structured Streaming windows by `createdAt` (server time); `foreachBatch` upserts absolute counts | A client clock could otherwise move the watermark and drop every later event. A forced batch replay restored a count edited by hand. `timestamptz` keeps instants right in any JVM time zone. |
| Observability (#13) | OpenTelemetry SDK + zio-telemetry with a `FiberRef` context, no Java agent | With the agent on JDK 27, 400 of 400 concurrent requests lost the caller's trace at the HTTP boundary; with fiber context, 400 of 400 kept it. zio-http's server dropped a wrapped not-found handler, so 404s get a real catch-all route. |
| Load test (#14) | Gatling (Java DSL from Scala 3); the pipe processes partitions in parallel | The pipe topped out at 24 events/s (one sequential stream). In parallel: baseline p95 12.2 s → 335 ms, end to end 42 s → 5.1 s. The first probe design loaded the gate it measured (68 % of requests) and was replaced by a by-id lookup. |
| Kubernetes (#15) | Helm chart with optional in-cluster infrastructure, installed on kind in CI | Kafka's readiness probe through its own Service could never pass (fixed with a TCP probe); init containers replace crash-restarts while dependencies start. |

## Open

- Under load the gate's Kafka producer waits longer for acknowledgements than the pipe's on the same broker (p50 59 ms
  vs 8 ms), so the load test's baseline limits (POST p95 < 500 ms, end to end < 3 s) are not met yet.
