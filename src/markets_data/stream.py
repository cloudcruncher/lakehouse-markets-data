"""Spark transforms for the trades stream: Kafka records -> typed trades with a reject reason.

Kept apart from the job so a plain local Spark session can check them (tests/spark), without
Kafka, Polaris or storage.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from markets_data import trades


def raw(kafka: DataFrame) -> DataFrame:
    """Kafka records as bronze keeps them: the payload untouched, plus where it came from."""
    return kafka.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    )


def parse(bronze: DataFrame) -> DataFrame:
    """Typed trades, each with `reject_reason` (NULL when valid)."""
    t = F.from_json("payload", trades.VALUE_SCHEMA)
    product = F.split(t["product_id"], "-")
    return bronze.select(
        "kafka_key",
        "payload",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingested_at",
        t["trade_id"].alias("trade_id"),
        t["product_id"].alias("product_id"),
        product.getItem(0).alias("base_currency"),
        product.getItem(1).alias("quote_currency"),
        t["price"].cast(trades.DECIMAL).alias("price"),
        t["size"].cast(trades.DECIMAL).alias("size"),
        t["side"].alias("side"),
        t["time"].cast("timestamp").alias("trade_time"),
        t["sequence"].alias("sequence"),
    ).withColumn("reject_reason", F.expr(trades.reject_reason_sql()))


def valid(parsed: DataFrame) -> DataFrame:
    """Each valid trade once: Coinbase resends the latest trade after a reconnect."""
    return (
        parsed.filter("reject_reason IS NULL")
        .dropDuplicates(["product_id", "trade_id"])
        .select(*trades.SILVER_COLUMNS)
    )


def rejected(parsed: DataFrame) -> DataFrame:
    return parsed.filter("reject_reason IS NOT NULL").select(
        F.col("reject_reason").alias("reason"),
        "kafka_key",
        "payload",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.current_timestamp().alias("rejected_at"),
    )
