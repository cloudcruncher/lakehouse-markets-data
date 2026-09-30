"""The Kappa stream for Coinbase trades (spark-submit, as this tenant; markets_data.scale says when).

Two queries read markets.coinbase.trades, each with its own checkpoint under CHECKPOINTS (the
service's /state volume, so a restart resumes where it stopped):

  * bronze: every record as it arrived, MERGEd on (partition, offset) so a stale checkpoint adds nothing twice
  * silver: typed and checked; valid trades MERGEd once by (product_id, trade_id), the rest
    MERGEd into markets_bronze.trades_rejects with a reason. MERGE keeps a retried batch from
    writing anything twice.

Bronze is append-only, so without a checkpoint (a new /state volume) it starts just past the
offsets it already holds rather than appending the topic's 7 days again. Silver starts from the
earliest record and MERGE skips what it has.

Replay (the Kappa part): a new table and checkpoint rebuild from the topic, which keeps 7 days.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from markets_data import stream, trades
from markets_data.scale import trigger_kwargs
from markets_data.spark import session
from markets_data.streaming import CHECKPOINTS, kafka, merge_bronze, resume_start, watch


def merge_trades(good: DataFrame, table: str) -> None:
    """MERGE valid trades into a silver table once each (a retried batch writes nothing twice)."""
    oldest = good.agg(F.min("trade_time")).first()[0]
    if oldest is None:
        return
    good.createOrReplaceTempView("trades_batch")
    # Only the days this batch touches (the table is partitioned by day of trade_time).
    good.sparkSession.sql(f"""
        MERGE INTO {table} t USING trades_batch s
        ON t.product_id = s.product_id AND t.trade_id = s.trade_id
           AND t.trade_time >= TIMESTAMP '{oldest:%Y-%m-%d}'
        WHEN NOT MATCHED THEN INSERT *
    """)


def merge_silver(batch: DataFrame, batch_id: int) -> None:
    spark = batch.sparkSession
    parsed = stream.parse(stream.raw(batch)).persist()
    good, bad = stream.valid(parsed), stream.rejected(parsed)
    bad.createOrReplaceTempView("rejects_batch")
    # Silver is whichever table holds the name: the original layout, or v2's after a replay swap
    # (markets_data.jobs.trades_swap). Writing what the table has lets a release ship before the swap.
    if "notional" in spark.table(trades.SILVER).columns:
        good = stream.with_notional(good)
    merge_trades(good, trades.SILVER)
    n_bad = bad.count()
    if n_bad:
        spark.sql(f"""
            MERGE INTO {trades.REJECTS} t USING rejects_batch s
            ON t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset
            WHEN NOT MATCHED THEN INSERT *
        """)
    print(f"[trades] batch {batch_id}: {good.count()} trades, {n_bad} rejected", flush=True)
    parsed.unpersist()


def start(spark: SparkSession) -> list[StreamingQuery]:
    """Create the tables if needed and start both queries (bronze, silver)."""
    for ddl in (trades.BRONZE_DDL, trades.REJECTS_DDL, trades.SILVER_DDL):
        spark.sql(ddl)
    bronze_cp, silver_cp = f"{CHECKPOINTS}/trades_bronze", f"{CHECKPOINTS}/trades_silver"
    bronze = (
        kafka(spark, trades.TOPIC, resume_start(spark, bronze_cp, trades.TOPIC, [trades.BRONZE]))
        .writeStream.queryName("trades_bronze")
        .option("checkpointLocation", bronze_cp)
        .trigger(**trigger_kwargs())
        .foreachBatch(merge_bronze(trades.BRONZE, stream.raw))
        .start()
    )
    silver = (
        kafka(
            spark, trades.TOPIC, resume_start(spark, silver_cp, trades.TOPIC, [trades.SILVER, trades.REJECTS])
        )
        .writeStream.queryName("trades_silver")
        .option("checkpointLocation", silver_cp)
        .trigger(**trigger_kwargs())
        .foreachBatch(merge_silver)
        .start()
    )
    print(f"[trades] streaming {trades.TOPIC} -> {trades.BRONZE}, {trades.SILVER}", flush=True)
    return [bronze, silver]


def main() -> None:
    watch(*start(session("markets-trades-stream")))


if __name__ == "__main__":
    main()
