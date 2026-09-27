"""A Spark session as this tenant: the platform's Polaris REST catalog, with its own principal.

Built only on the platform's published interface (ADR 14): the base image's jars, the
`polaris` host on the `open-lakehouse_data` network, and the credentials file the platform
mounts for this tenant alone. No storage keys: Polaris vends short-lived, table-scoped ones.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession

CATALOG = "lakehouse"
POLARIS = os.environ.get("POLARIS_URI", "http://polaris:8181/api/catalog")


def credentials(path: str | None = None) -> tuple[str, str]:
    text = Path(path or os.environ["POLARIS_ENV_FILE"]).read_text()
    env = dict(line.split("=", 1) for line in text.splitlines() if "=" in line)
    return env["POLARIS_CLIENT_ID"], env["POLARIS_CLIENT_SECRET"]


def session(app: str) -> SparkSession:
    client_id, secret = credentials()
    c = f"spark.sql.catalog.{CATALOG}"
    spark = (
        SparkSession.builder.appName(app)
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
