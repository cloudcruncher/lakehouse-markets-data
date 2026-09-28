"""What every Kappa stream in this repo shares: its Kafka source, where bronze resumes, and the
watch loop that ends the job when a query fails (the platform then restarts the service).

Runs inside spark-submit; the transforms each stream applies live next to their table layouts,
so a plain local Spark session can check them (tests/spark).
"""

from __future__ import annotations

import json
import os
import time

from confluent_kafka.admin import AdminClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

CHECKPOINTS = os.environ.get("CHECKPOINTS", "/state/checkpoints")
TRIGGER = os.environ.get("TRIGGER", "30 seconds")
MAX_OFFSETS = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000"))
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")


def bronze_start(spark: SparkSession, checkpoint: str, topic: str, table: str) -> str:
    """Kafka offsets for bronze: the checkpoint's if it has one, else just past the table's."""
    if os.path.isdir(f"{checkpoint}/offsets"):
        return "earliest"  # ignored by Spark: a checkpoint always wins
    held = {
        r.kafka_partition: r.next_offset
        for r in spark.sql(
            f"SELECT kafka_partition, max(kafka_offset) + 1 AS next_offset FROM {table} GROUP BY 1"
        ).collect()
    }
    if not held:
        return "earliest"
    partitions = AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(topic, timeout=10)
    # -2 is Kafka's "earliest", for partitions bronze has no record from yet.
    offsets = {str(p): held.get(p, -2) for p in partitions.topics[topic].partitions}
    print(f"[{table}] no bronze checkpoint: resuming after the table's offsets {offsets}", flush=True)
    return json.dumps({topic: offsets})


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
    """Block while every query runs; any one failing stops the job, and the platform restarts it."""
    while all(q.isActive for q in queries):
        time.sleep(10)
    for q in queries:
        if q.exception():
            raise q.exception()
        q.stop()
