"""A Spark session as this tenant: the platform's Polaris REST catalog, with its own principal.

Built only on the platform's published interface (ADR 14): the base image's jars, the
`polaris` host on the `open-lakehouse_data` network, and the credentials file the platform
mounts for this tenant alone. No storage keys: Polaris vends short-lived, table-scoped ones.
"""

from __future__ import annotations

import os
import socket
import sys
from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import SparkSession

CATALOG = "lakehouse"
POLARIS = os.environ.get("POLARIS_URI", "http://polaris:8181/api/catalog")


def credentials(path: str | None = None) -> tuple[str, str]:
    text = Path(path or os.environ["POLARIS_ENV_FILE"]).read_text()
    env = dict(line.split("=", 1) for line in text.splitlines() if "=" in line)
    return env["POLARIS_CLIENT_ID"], env["POLARIS_CLIENT_SECRET"]


def lineage(builder: SparkSession.Builder, app: str) -> SparkSession.Builder:
    """OpenLineage events to the platform's Marquez (OPENLINEAGE_URL), when it is up.

    Lineage is observability, never a dependency: without Marquez the job runs anyway.
    """
    url = os.environ.get("OPENLINEAGE_URL", "")
    if not url:
        return builder
    u = urlparse(url)
    try:
        socket.create_connection((u.hostname, u.port or 80), timeout=0.5).close()
    except OSError:
        print(f"[lineage] {url} unreachable; running without lineage events", file=sys.stderr)
        return builder
    return (
        builder.config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", url)
        .config("spark.openlineage.namespace", os.environ.get("TENANT", "markets-data"))
        .config("spark.openlineage.parentJobName", app)
        # Its Iceberg metrics hook fails on every streaming batch (OpenLineage#4950); lineage
        # itself is unaffected.
        .config("spark.openlineage.vendors.iceberg.metricsReporterDisabled", "true")
    )


def session(app: str) -> SparkSession:
    client_id, secret = credentials()
    c = f"spark.sql.catalog.{CATALOG}"
    spark = (
        lineage(SparkSession.builder.appName(app), app)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(c, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{c}.type", "rest")
        .config(f"{c}.uri", POLARIS)
        .config(f"{c}.warehouse", CATALOG)
        .config(f"{c}.credential", f"{client_id}:{secret}")
        .config(f"{c}.scope", "PRINCIPAL_ROLE:ALL")
        .config(f"{c}.rest.auth.type", "oauth2")
        .config(f"{c}.oauth2-server-uri", f"{POLARIS}/v1/oauth/tokens")
        .config(f"{c}.token-refresh-enabled", "true")
        .config(f"{c}.header.X-Iceberg-Access-Delegation", "vended-credentials")
        .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{c}.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
