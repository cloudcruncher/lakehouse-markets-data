"""What every Kappa stream in this repo shares: its Kafka source, where bronze resumes, and the
watch loop that ends the job when its queries finish (a scheduled catch-up, markets_data.scale)
or one fails (the platform then restarts the service).

Runs inside spark-submit; the transforms each stream applies live next to their table layouts,
so a plain local Spark session can check them (tests/spark).
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable

from confluent_kafka.admin import AdminClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from markets_data import bronze

CHECKPOINTS = os.environ.get("CHECKPOINTS", "/state/checkpoints")
# A batch is capped at 10,000 records, which bounds its memory; a catch-up reads in several.
MAX_OFFSETS = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "10000"))
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")


def resume_start(spark: SparkSession, checkpoint: str, topic: str, tables: list[str]) -> str:
    """Kafka offsets for a query: its checkpoint's if it has one, else just past the offsets its
    tables already hold (a new /state volume then costs no re-read of the topic's 7 days)."""
    if os.path.isdir(f"{checkpoint}/offsets"):
        return "earliest"  # ignored by Spark: a checkpoint always wins
    union = " UNION ALL ".join(f"SELECT kafka_partition, kafka_offset FROM {t}" for t in tables)
    held = {
        r.kafka_partition: r.next_offset
        for r in spark.sql(
            f"SELECT kafka_partition, max(kafka_offset) + 1 AS next_offset FROM ({union}) GROUP BY 1"
        ).collect()
    }
    if not held:
        return "earliest"
    partitions = AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(topic, timeout=10)
    # -2 is Kafka's "earliest", for partitions bronze has no record from yet.
    offsets = {str(p): held.get(p, -2) for p in partitions.topics[topic].partitions}
    print(f"[{tables[0]}] no checkpoint: resuming after the tables' offsets {offsets}", flush=True)
    return json.dumps({topic: offsets})


def merge_bronze(table: str, raw: Callable[[DataFrame], DataFrame]) -> Callable[[DataFrame, int], None]:
    """A foreachBatch function writing each Kafka record to `table` once (markets_data.bronze)."""

    def write(batch: DataFrame, batch_id: int) -> None:
        rows = raw(batch)
        oldest, min_offset = rows.agg(F.min("kafka_timestamp"), F.min("kafka_offset")).first()
        if oldest is None:
            return
        rows.createOrReplaceTempView("bronze_batch")
        rows.sparkSession.sql(bronze.merge_sql(table, oldest, min_offset))

    return write


def kafka(spark: SparkSession, topic: str, starting: str = "earliest") -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", starting)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        # Retention may delete records a stopped stream never read: note it, don't stop.
        .option("failOnDataLoss", "false")
        .load()
    )


def watch(*queries: StreamingQuery) -> None:
    """Block until every query has finished (a catch-up run) or any one fails, which stops the
    rest and the job; the platform then restarts it."""
    while any(q.isActive for q in queries):
        if failed := next((q for q in queries if q.exception()), None):
            for q in queries:
                q.stop()
            raise failed.exception()
        time.sleep(5)
    for q in queries:
        if q.exception():
            raise q.exception()
