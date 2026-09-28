"""Build the gold data products from silver (run by spark-submit, as this tenant, hourly).

  * markets_gold.crypto_ohlcv_1m and markets_gold.card_auth_daily: the last REBUILD_DAYS days
    are recomputed and their day partitions replaced in one commit each (everything, on the
    first build). Idempotent: a re-run gives the same rows.
  * markets_gold.sanctions_hits: every merchant screened against today's list, the table
    replaced whole, so a merchant whose match is delisted drops out.

Then each check in markets_data.gold.CHECKS runs and is reported to Dagster.
"""

from __future__ import annotations

import os
from datetime import datetime, time, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from markets_data import card_auths, gold, sanctions, trades
from markets_data.spark import session


def rebuild_from(spark: SparkSession, table: str) -> datetime:
    """Midnight UTC REBUILD_DAYS - 1 days ago, or the epoch when the table is empty."""
    if spark.table(table).isEmpty():
        return datetime(1970, 1, 1)
    today = datetime.now(timezone.utc).date()
    return datetime.combine(today - timedelta(days=gold.REBUILD_DAYS - 1), time())


def build(spark: SparkSession, table: str, sql: str, source: str) -> tuple[str, int]:
    since = rebuild_from(spark, table)
    rows = spark.sql(sql.format(source=source, since=gold.since_sql(f"{since:%Y-%m-%d %H:%M:%S}")))
    rows.writeTo(table).overwritePartitions()  # only the day partitions the rebuild produced
    n = spark.table(table).count()
    print(f"[gold] {table}: rebuilt from {since:%Y-%m-%d}, {n} rows", flush=True)
    return f"{since:%Y-%m-%d %H:%M:%S}", n


def screen(spark: SparkSession) -> int:
    merchants = [
        {**r.asDict(), "name_norm": sanctions.normalise(r.merchant_name)}
        for r in spark.sql(gold.MERCHANTS_SQL).collect()
    ]
    schema = spark.sql(gold.MERCHANTS_SQL).schema.add("name_norm", "string")
    spark.createDataFrame(merchants, schema).createOrReplaceTempView("screened_merchants")
    hits = spark.sql(gold.HITS_SQL.format(merchants="screened_merchants", names=sanctions.SILVER))
    hits.writeTo(gold.HITS).overwrite(F.lit(True))  # the whole table, even when there are no hits
    n = spark.table(gold.HITS).count()
    print(f"[gold] {gold.HITS}: {len(merchants)} merchants screened, {n} hits", flush=True)
    return n


def main() -> None:
    spark = session("markets-gold")
    for ddl in (gold.OHLCV_DDL, gold.CARD_DAILY_DDL, gold.HITS_DDL):
        spark.sql(ddl)

    _, candles = build(spark, gold.OHLCV, gold.OHLCV_SQL, trades.SILVER)
    card_since, days = build(spark, gold.CARD_DAILY, gold.CARD_DAILY_SQL, card_auths.SILVER)
    hits = screen(spark)

    results = []
    for name, table, sql in gold.CHECKS:
        bad = spark.sql(
            sql.format(since=gold.since_sql(card_since), since_date=f"DATE '{card_since[:10]}'")
        ).first()[0]
        results.append((name, table, bad))
        print(f"[gold] check {name}: {'PASS' if bad == 0 else f'FAIL ({bad})'}", flush=True)

    if "DAGSTER_PIPES_CONTEXT" in os.environ:
        from dagster_pipes import open_dagster_pipes

        key = {t: t.replace(".", "/") for t in (gold.OHLCV, gold.CARD_DAILY, gold.HITS)}
        with open_dagster_pipes() as pipes:
            for table, rows in ((gold.OHLCV, candles), (gold.CARD_DAILY, days), (gold.HITS, hits)):
                pipes.report_asset_materialization(asset_key=key[table], metadata={"rows": rows})
            for name, table, bad in results:
                pipes.report_asset_check(
                    check_name=name, passed=bad == 0, asset_key=key[table], metadata={"offending_rows": bad}
                )
    elif any(bad for *_, bad in results):
        raise SystemExit("[gold] a check failed")


if __name__ == "__main__":
    main()
