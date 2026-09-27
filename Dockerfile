# Built on the platform's published Spark image (ADR 14): Spark, Iceberg, Kafka and
# OpenLineage jars, Dagster and dagster-pipes are already there. Pin the platform release
# this repo is tested against; bump it in a PR like any dependency.
FROM ghcr.io/cloudcruncher/open-lakehouse-spark:0.3.0

USER root
COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock README.md /opt/markets-data/
# Only the libraries this repo adds (pyproject `dependencies`), pinned and hash-checked from
# uv.lock; everything else is the base image's, so nothing unreviewed is pulled in.
RUN cd /opt/markets-data \
 && uv export --frozen --no-dev --no-emit-project --no-header -o /tmp/requirements.txt \
 && uv pip install --system --break-system-packages --no-cache --require-hashes -r /tmp/requirements.txt \
 && rm /tmp/requirements.txt
COPY src /opt/markets-data/src
RUN uv pip install --system --break-system-packages --no-deps --no-cache /opt/markets-data
# Local runs only: the platform mounts its own dagster.yaml over this one when it deploys us.
RUN printf 'telemetry:\n  enabled: false\n' > /opt/dagster/home/dagster.yaml && chown spark /opt/dagster/home/dagster.yaml
ENV DAGSTER_HOME=/opt/dagster/home
USER spark
WORKDIR /opt/markets-data
ENTRYPOINT ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "markets_data.definitions"]
