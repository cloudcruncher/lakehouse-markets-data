"""The Kappa stream for Coinbase trades (run by spark-submit, as this tenant, for as long as it runs).

Two queries read markets.coinbase.trades, each with its own checkpoint under CHECKPOINTS (the
service's /state volume, so a restart resumes where it stopped):

  * bronze: every record as it arrived, appended (Iceberg commits are exactly once per batch)
  * silver: typed and checked; valid trades MERGEd once by (product_id, trade_id), the rest
    MERGEd into markets_bronze.trades_rejects with a reason. MERGE keeps a retried batch from
    writing anything twice.

Bronze is append-only, so without a checkpoint (a new /state volume) it starts just past the
offsets it already holds rather than appending the topic's 7 days again. Silver starts from the
earliest record and MERGE skips what it has.

Replay (the Kappa part): a new table and checkpoint rebuild from the topic, which keeps 7 days.
"""

from __future__ import annotations

import json
import os
import time

from confluent_kafka.admin import AdminClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from markets_data import stream, trades
from markets_data.spark import session

CHECKPOINTS = os.environ.get("CHECKPOINTS", "/state/checkpoints")
TRIGGER = os.environ.get("TRIGGER", "30 seconds")
MAX_OFFSETS = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000"))
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")


def bronze_start(spark: SparkSession, checkpoint: str) -> str:
    """Kafka offsets for bronze: the checkpoint's if it has one, else just past the table's."""
    if os.path.isdir(f"{checkpoint}/offsets"):
        return "earliest"  # ignored by Spark: a checkpoint always wins
    held = {
        r.kafka_partition: r.next_offset
        for r in spark.sql(
            f"SELECT kafka_partition, max(kafka_offset) + 1 AS next_offset FROM {trades.BRONZE} GROUP BY 1"
        ).collect()
    }
    if not held:
        return "earliest"
    topic = AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(trades.TOPIC, timeout=10)
    # -2 is Kafka's "earliest", for partitions bronze has no record from yet.
    offsets = {str(p): held.get(p, -2) for p in topic.topics[trades.TOPIC].partitions}
    print(f"[trades] no bronze checkpoint: resuming after the table's offsets {offsets}", flush=True)
    return json.dumps({trades.TOPIC: offsets})


def kafka(spark: SparkSession, starting: str = "earliest") -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", trades.TOPIC)
        .option("startingOffsets", starting)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        # Retention may delete records a stopped stream never read: note it, don't stop.
        .option("failOnDataLoss", "false")
        .load()
    )


def merge_silver(batch: DataFrame, batch_id: int) -> None:
    spark = batch.sparkSession
    parsed = stream.parse(stream.raw(batch)).persist()
    good, bad = stream.valid(parsed), stream.rejected(parsed)
    good.createOrReplaceTempView("trades_batch")
    bad.createOrReplaceTempView("rejects_batch")
    oldest = good.agg(F.min("trade_time")).first()[0]
    if oldest is not None:
        # Only the days this batch touches (the table is partitioned by day of trade_time).
        spark.sql(f"""
            MERGE INTO {trades.SILVER} t USING trades_batch s
            ON t.product_id = s.product_id AND t.trade_id = s.trade_id
               AND t.trade_time >= TIMESTAMP '{oldest:%Y-%m-%d}'
            WHEN NOT MATCHED THEN INSERT *
        """)
    n_bad = bad.count()
    if n_bad:
        spark.sql(f"""
            MERGE INTO {trades.REJECTS} t USING rejects_batch s
            ON t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset
            WHEN NOT MATCHED THEN INSERT *
        """)
    print(f"[trades] batch {batch_id}: {good.count()} trades, {n_bad} rejected", flush=True)
    parsed.unpersist()


def main() -> None:
    spark = session("markets-trades-stream")
    for ddl in (trades.BRONZE_DDL, trades.REJECTS_DDL, trades.SILVER_DDL):
        spark.sql(ddl)

    checkpoint = f"{CHECKPOINTS}/trades_bronze"
    bronze = (
        stream.raw(kafka(spark, bronze_start(spark, checkpoint)))
        .writeStream.queryName("trades_bronze")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=TRIGGER)
        .toTable(trades.BRONZE)
    )
    silver = (
        kafka(spark)
        .writeStream.queryName("trades_silver")
        .option("checkpointLocation", f"{CHECKPOINTS}/trades_silver")
        .trigger(processingTime=TRIGGER)
        .foreachBatch(merge_silver)
        .start()
    )
    print(f"[trades] streaming {trades.TOPIC} -> {trades.BRONZE}, {trades.SILVER}", flush=True)
    # Either query failing stops the job, and the platform restarts the service.
    while bronze.isActive and silver.isActive:
        time.sleep(10)
    for q in (bronze, silver):
        if q.exception():
            raise q.exception()
        q.stop()


if __name__ == "__main__":
    main()
