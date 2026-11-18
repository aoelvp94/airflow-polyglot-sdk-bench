# Airflow 3.4.0 + JRE 17 + Node 22 + the Go/Java/TS task-handler bundles + dbt.
#
# Each language bundle is built in its own stage and copied into a Dag bundle
# directory under /opt/airflow/handlers/<lang>. Each directory carries an
# .airflowignore of "*" so the Dag processor never tries to parse it; the
# coordinators find the executables there by scanning the bundle.

# ---- Go: one self-contained executable (AFBNDL01) -----------------------------
FROM golang:1.25-bookworm AS go-build
WORKDIR /src
COPY handlers/go/go.mod handlers/go/go.sum ./
RUN go mod download
COPY handlers/go/*.go ./
RUN go tool airflow-go-pack --output /out/ingest_handlers . -- -trimpath -ldflags="-s -w"

# ---- Java: app JAR (Main-Class) + dependency JARs, unshaded ------------------
FROM maven:3.9-eclipse-temurin-17 AS java-build
WORKDIR /src
COPY handlers/java/pom.xml ./
RUN mvn -q -B dependency:resolve dependency:resolve-plugins
COPY handlers/java/src ./src
RUN mvn -q -B package \
 && mkdir /out && cp target/ingest-handlers.jar target/bundle/*.jar /out/

# ---- TypeScript: one packed bundle.min.mjs ------------------------------------
FROM node:22-bookworm-slim AS ts-build
WORKDIR /src
COPY handlers/ts/package.json handlers/ts/package-lock.json handlers/ts/tsconfig.json ./
RUN npm ci --no-audit --no-fund
COPY handlers/ts/src ./src
RUN npx tsc --noEmit && npx airflow-ts-pack src/main.ts --outdir /out

FROM eclipse-temurin:17-jre AS jre
FROM node:22-bookworm-slim AS node

# ---- Runtime -------------------------------------------------------------------
FROM apache/airflow:3.4.0-python3.12
USER root
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
COPY --from=node /usr/local/bin/node /usr/local/bin/node
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:${PATH}

COPY --from=go-build   --chown=airflow:0 /out/ingest_handlers /opt/airflow/handlers/go/ingest_handlers
COPY --from=java-build --chown=airflow:0 /out/                 /opt/airflow/handlers/java/
COPY --from=ts-build   --chown=airflow:0 /out/bundle.min.mjs   /opt/airflow/handlers/ts/bundle.min.mjs
RUN for d in go java ts; do printf '*\n' > /opt/airflow/handlers/$d/.airflowignore; done \
 && chown -R airflow:0 /opt/airflow/handlers \
 && java -version && node --version

USER airflow
# Cosmos goes into Airflow's environment; dbt gets its own venv so its pins
# never fight Airflow's (Cosmos calls it as a subprocess via dbt_executable_path).
RUN pip install --no-cache-dir "apache-airflow==3.4.0" "astronomer-cosmos==1.16.0"
RUN python -m venv /opt/airflow/dbt-venv \
 && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.12.7" "dbt-postgres==1.11.0"

# The dbt project ships with a pre-built manifest so Cosmos (LoadMode.DBT_MANIFEST)
# renders the task group without running dbt at Dag-parse time.
COPY --chown=airflow:0 dbt/orders /opt/airflow/dbt/orders
RUN cd /opt/airflow/dbt/orders \
 && /opt/airflow/dbt-venv/bin/dbt parse --profiles-dir . --target local \
 && test -f target/manifest.json
