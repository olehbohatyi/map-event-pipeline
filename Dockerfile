# syntax=docker/dockerfile:1.7
# One build stage, three runtime targets: gate, pipe, stat
#   docker build --target gate -t map-event-gate .

# ── build ────────────────────────────────────────────────────────────────────
FROM eclipse-temurin:27-jdk AS build
ARG SBT_VERSION=1.13.0
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
    sbt -batch update

COPY core/src/main core/src/main
COPY gate/src/main gate/src/main
COPY pipe/src/main pipe/src/main
COPY stat/src/main stat/src/main
RUN --mount=type=cache,target=/root/.cache/coursier \
    --mount=type=cache,target=/root/.sbt \
    --mount=type=cache,target=/root/.ivy2 \
    sbt -batch gate/stage pipe/stage stat/assembly

# ── gate: HTTP API ───────────────────────────────────────────────────────────
FROM eclipse-temurin:27-jre AS gate
RUN groupadd --system mep && useradd --system --gid mep mep
WORKDIR /app
COPY --from=build --chown=mep:mep /src/gate/target/universal/stage/ ./
USER mep
EXPOSE 8080
ENTRYPOINT ["bin/gate"]

# ── pipe: Kafka stream processor ─────────────────────────────────────────────
FROM eclipse-temurin:27-jre AS pipe
RUN groupadd --system mep && useradd --system --gid mep mep
WORKDIR /app
COPY --from=build --chown=mep:mep /src/pipe/target/universal/stage/ ./
USER mep
ENTRYPOINT ["bin/pipe"]

# ── stat: Spark batch job (Spark 4.x runs on Java 21) ────────────────────────
FROM apache/spark:4.2.0-scala2.13-java21-ubuntu AS stat
COPY --from=build /src/stat/target/stat.jar /opt/mep/stat.jar
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[*]", \
            "--class", "com.github.olehbohatyi.mep.stat.StatApp", "/opt/mep/stat.jar"]
