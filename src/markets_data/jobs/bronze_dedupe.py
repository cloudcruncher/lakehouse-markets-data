"""Remove duplicate records from the bronze tables: one row per (kafka_partition, kafka_offset).

Before bronze was written with a MERGE (markets_data.bronze), a checkpoint older than the table made a
stream append offsets the table already held. This repairs that, and is safe to run any time: it finds
the day-partitions that contain duplicates, rewrites only those with the earliest ingest of each record
(one atomic Iceberg snapshot, so the previous state stays readable by time travel), and does nothing
when there are none. Run it with the streams idle (between catch-ups), so no write races the rewrite.
"""

from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from markets_data import card_auths, trades
from markets_data.spark import session

TABLES = (trades.BRONZE, card_auths.BRONZE)
KEY = ["kafka_partition", "kafka_offset"]


def main() -> None:
    spark = session("markets-bronze-dedupe")
    for table in TABLES:
        df = spark.table(table)
        days = (
            df.groupBy(*KEY, F.to_date("kafka_timestamp").alias("day"))
            .count()
            .filter("count > 1")
            .select("day")
            .distinct()
            .collect()
        )
        if not days:
            print(f"[dedupe] {table}: no duplicates", flush=True)
            continue
        touched = [d.day for d in days]
        scope = df.filter(F.to_date("kafka_timestamp").isin(touched))
        before = scope.count()
        first = Window.partitionBy(*KEY).orderBy("ingested_at")
        kept = scope.withColumn("_n", F.row_number().over(first)).filter("_n = 1").drop("_n").persist()
        after = kept.count()
        kept.writeTo(table).overwritePartitions()
        spark.catalog.refreshTable(table)
        print(f"[dedupe] {table}: {len(touched)} day(s) rewritten, {before} -> {after} rows", flush=True)
        kept.unpersist()


if __name__ == "__main__":
    main()
