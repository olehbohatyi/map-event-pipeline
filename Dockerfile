# syntax=docker/dockerfile:1.7
# One build stage, three runtime targets: gate, pipe, stat
#   docker build --target gate -t map-event-gate .

# ── build ────────────────────────────────────────────────────────────────────
FROM eclipse-temurin:27-jdk AS build
ARG SBT_VERSION=2.0.11
# Retry flaky artifact downloads instead of failing the build
ENV COURSIER_RETRY=5
ADD https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz /tmp/sbt.tgz
RUN tar -xzf /tmp/sbt.tgz -C /opt && rm /tmp/sbt.tgz && ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt
WORKDIR /src

# Resolve dependencies first so source edits don't invalidate this layer
COPY build.sbt ./
COPY project/build.properties project/plugins.sbt project/Dependencies.scala project/
RUN --mount=type=cache,target=/root/.cache/coursier \
    --mount=type=cache,target=/root/.sbt \
    --mount=type=cache,target=/root/.ivy2 \
    sbt -batch --server update

COPY core/src/main core/src/main
COPY gate/src/main gate/src/main
COPY pipe/src/main pipe/src/main
COPY stat/src/main stat/src/main
RUN --mount=type=cache,target=/root/.cache/coursier \
    --mount=type=cache,target=/root/.sbt \
    --mount=type=cache,target=/root/.ivy2 \
    sbt -batch --server "gate/stage; pipe/stage; stat/assembly"
# sbt 2 writes to target/out/jvm/scala-<version>/<project>/; copy to fixed paths so a Scala bump does not break the COPYs
RUN mkdir -p /out \
 && cp -r target/out/jvm/scala-*/gate/universal/stage /out/gate \
 && cp -r target/out/jvm/scala-*/pipe/universal/stage /out/pipe \
 && cp target/out/jvm/scala-*/stat/stat.jar /out/stat.jar

# ── gate: HTTP API ───────────────────────────────────────────────────────────
FROM eclipse-temurin:27-jre AS gate
RUN groupadd --system mep && useradd --system --gid mep mep
WORKDIR /app
COPY --from=build --chown=mep:mep /out/gate/ ./
USER mep
EXPOSE 8080
ENTRYPOINT ["bin/gate"]

# ── pipe: Kafka stream processor ─────────────────────────────────────────────
FROM eclipse-temurin:27-jre AS pipe
RUN groupadd --system mep && useradd --system --gid mep mep
WORKDIR /app
COPY --from=build --chown=mep:mep /out/pipe/ ./
USER mep
ENTRYPOINT ["bin/pipe"]

# ── stat: Spark batch and streaming jobs (Spark 4.x runs on Java 21) ─────────
FROM apache/spark:4.2.0-scala2.13-java21-ubuntu AS stat
COPY --from=build /out/stat.jar /opt/mep/stat.jar
# The streaming job's checkpoint. Created here, owned by the image's `spark` user, so that a new named volume mounted
# on it inherits that owner and the job can write to it
USER root
RUN mkdir -p /var/lib/mep/trends && chown spark:spark /var/lib/mep/trends
USER spark
# The batch job by default; compose runs the streaming job with `--class ...TrendsApp /opt/mep/stat.jar`
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[*]"]
CMD ["--class", "com.github.olehbohatyi.mep.stat.StatApp", "/opt/mep/stat.jar"]
