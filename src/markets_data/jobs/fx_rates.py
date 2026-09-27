"""Land ECB reference rates in markets_bronze.fx_rates (run by spark-submit, as this tenant).

Incremental and idempotent: each run re-reads the last week (the ECB occasionally corrects a
fixing) and MERGEs by (rate_date, base, quote), so re-runs and overlaps never duplicate rows.
The first run backfills from FIRST_DATE.
"""

from __future__ import annotations

import os
from datetime import date, timedelta

from pyspark.sql import functions as F

from markets_data import fx
from markets_data.spark import session

TABLE = "markets_bronze.fx_rates"
FIRST_DATE = date.fromisoformat(os.environ.get("FX_FIRST_DATE", "2025-01-01"))
REVISIT_DAYS = 7


def main() -> None:
    spark = session("markets-fx-rates")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            rate_date date, base string, quote string, rate double,
            source string, ingested_at timestamp
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
    """)
    last = spark.sql(f"SELECT max(rate_date) FROM {TABLE}").first()[0]
    start = last - timedelta(days=REVISIT_DAYS) if last else FIRST_DATE
    rows = fx.to_rows(fx.fetch(start, date.today()))
    print(f"[fx] fetched {len(rows)} rates for {start}..{date.today()}", flush=True)

    src = (
        spark.createDataFrame(rows, "rate_date date, base string, quote string, rate double")
        .withColumn("source", F.lit(fx.SOURCE))
        .withColumn("ingested_at", F.current_timestamp())
    )
    src.createOrReplaceTempView("fx_src")
    before = spark.table(TABLE).count()
    spark.sql(f"""
        MERGE INTO {TABLE} t USING fx_src s
        ON t.rate_date = s.rate_date AND t.base = s.base AND t.quote = s.quote
        WHEN MATCHED AND t.rate <> s.rate THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    total = spark.table(TABLE).count()
    latest = spark.sql(f"SELECT max(rate_date) FROM {TABLE}").first()[0]
    print(f"[fx] {TABLE}: {total} rows (+{total - before}), latest {latest}", flush=True)

    if "DAGSTER_PIPES_CONTEXT" in os.environ:
        from dagster_pipes import open_dagster_pipes

        with open_dagster_pipes() as pipes:
            pipes.report_asset_materialization(
                metadata={"rows": total, "new_rows": total - before, "latest_rate_date": str(latest)}
            )


if __name__ == "__main__":
    main()
