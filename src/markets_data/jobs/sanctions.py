"""Land the UK Sanctions List (FCDO, via OpenSanctions) in bronze and its screening names in
silver (run by spark-submit, as this tenant, daily).

  * markets_bronze.sanctions_targets: one row per target ever listed, MERGEd by target_id.
    Changed targets are updated; targets gone from today's list get `delisted_at`, and come
    back (delisted_at NULL) if relisted. Nothing is deleted: screening history needs them.
  * markets_silver.sanctions_names: rebuilt each run from today's list, one row per
    normalised name or alias of each listed organisation (markets_data.sanctions).

Idempotent: a re-run on the same list changes nothing.
"""

from __future__ import annotations

import os

from pyspark.sql import functions as F

from markets_data import sanctions
from markets_data.spark import session

BRONZE_DDL = f"""
CREATE TABLE IF NOT EXISTS {sanctions.BRONZE} (
    target_id string, schema string, name string, aliases string, countries string,
    program_ids string, sanctions string, first_seen timestamp, last_seen timestamp,
    last_change timestamp, delisted_at timestamp, source string, ingested_at timestamp
) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""

SILVER_DDL = f"""
CREATE TABLE IF NOT EXISTS {sanctions.SILVER} (
    target_id string, schema string, name_norm string, countries string, program_ids string,
    list_date date
) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""

TARGET_SCHEMA = (
    "target_id string, schema string, name string, aliases string, countries string, "
    "program_ids string, sanctions string, first_seen timestamp, last_seen timestamp, last_change timestamp"
)


def main() -> None:
    spark = session("markets-sanctions")
    spark.sql(BRONZE_DDL)
    spark.sql(SILVER_DDL)

    rows = sanctions.to_rows(sanctions.fetch())
    names = sanctions.screening_names(rows)
    print(f"[sanctions] {len(rows)} targets, {len(names)} screening names from {sanctions.URL}", flush=True)
    if len(rows) < 1000:
        # The list has held ~6,000 targets for years: a tiny file is a broken download, and
        # MERGE would mark nearly everyone delisted.
        raise SystemExit(f"[sanctions] only {len(rows)} targets: refusing to treat that as the list")

    (
        spark.createDataFrame(rows, TARGET_SCHEMA)
        .withColumn("source", F.lit(sanctions.SOURCE))
        .withColumn("ingested_at", F.current_timestamp())
        .createOrReplaceTempView("sanctions_src")
    )
    listed_before = spark.sql(f"SELECT count(*) FROM {sanctions.BRONZE} WHERE delisted_at IS NULL").first()[0]
    spark.sql(f"""
        MERGE INTO {sanctions.BRONZE} t USING sanctions_src s ON t.target_id = s.target_id
        WHEN MATCHED AND (t.last_change IS DISTINCT FROM s.last_change OR t.delisted_at IS NOT NULL)
            THEN UPDATE SET t.schema = s.schema, t.name = s.name, t.aliases = s.aliases,
                t.countries = s.countries, t.program_ids = s.program_ids, t.sanctions = s.sanctions,
                t.first_seen = s.first_seen, t.last_seen = s.last_seen, t.last_change = s.last_change,
                t.delisted_at = NULL, t.source = s.source, t.ingested_at = s.ingested_at
        WHEN NOT MATCHED THEN INSERT (target_id, schema, name, aliases, countries, program_ids, sanctions,
                first_seen, last_seen, last_change, delisted_at, source, ingested_at)
            VALUES (s.target_id, s.schema, s.name, s.aliases, s.countries, s.program_ids, s.sanctions,
                s.first_seen, s.last_seen, s.last_change, NULL, s.source, s.ingested_at)
        WHEN NOT MATCHED BY SOURCE AND t.delisted_at IS NULL
            THEN UPDATE SET t.delisted_at = current_timestamp()
    """)
    listed = spark.sql(f"SELECT count(*) FROM {sanctions.BRONZE} WHERE delisted_at IS NULL").first()[0]
    delisted = spark.sql(f"SELECT count(*) FROM {sanctions.BRONZE} WHERE delisted_at IS NOT NULL").first()[0]

    (
        spark.createDataFrame(
            names, "target_id string, schema string, name_norm string, countries string, program_ids string"
        )
        .withColumn("list_date", F.current_date())
        .writeTo(sanctions.SILVER)
        .overwritePartitions()  # unpartitioned: replaces the whole table in one commit
    )
    print(f"[sanctions] {listed} listed ({listed - listed_before:+d}), {delisted} delisted ever", flush=True)

    if "DAGSTER_PIPES_CONTEXT" in os.environ:
        from dagster_pipes import open_dagster_pipes

        with open_dagster_pipes() as pipes:
            pipes.report_asset_materialization(
                asset_key="markets_bronze/sanctions_targets",
                metadata={
                    "listed": listed,
                    "listed_change": listed - listed_before,
                    "delisted_ever": delisted,
                },
            )
            pipes.report_asset_materialization(
                asset_key="markets_silver/sanctions_names", metadata={"screening_names": len(names)}
            )


if __name__ == "__main__":
    main()
