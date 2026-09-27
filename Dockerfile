# Built on the platform's published Spark image (ADR 14): Spark, Iceberg, Kafka and
# OpenLineage jars, Dagster and dagster-pipes are already there. Pin the platform release
# this repo is tested against; bump it in a PR like any dependency.
FROM ghcr.io/cloudcruncher/open-lakehouse-spark:0.2.0

USER root
COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/local/bin/uv
COPY pyproject.toml README.md /opt/markets-data/
COPY src /opt/markets-data/src
# --no-deps: runtime libraries come from the base image, so nothing unreviewed is pulled in.
RUN uv pip install --system --break-system-packages --no-deps --no-cache /opt/markets-data
# Local runs only: the platform mounts its own dagster.yaml over this one when it deploys us.
RUN printf 'telemetry:\n  enabled: false\n' > /opt/dagster/home/dagster.yaml && chown spark /opt/dagster/home/dagster.yaml
ENV DAGSTER_HOME=/opt/dagster/home
USER spark
WORKDIR /opt/markets-data
ENTRYPOINT ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "markets_data.definitions"]
