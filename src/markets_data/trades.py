"""Coinbase trades in the lakehouse: table names, layouts and the rules a trade must pass.

No Spark here, so the rules are unit-tested in seconds; the stream (markets_data.stream) applies
them as SQL. Kappa: Kafka is the source of truth, bronze keeps every record as it arrived,
silver keeps each valid trade once. Replaying the topic rebuilds both.
"""

TOPIC = "markets.coinbase.trades"
BRONZE = "markets_bronze.trades"
REJECTS = "markets_bronze.trades_rejects"
SILVER = "markets_silver.trades"

# The producer keeps price and size as strings; they become decimals here, once. 18 integer and
# 12 fractional digits hold any Coinbase price (up to ~1e6) and size (down to 1e-8).
DECIMAL = "decimal(30,12)"
VALUE_SCHEMA = (
    "trade_id bigint, product_id string, price string, size string, side string, time string, sequence bigint"
)

# First match wins; a trade that matches none is valid. Columns are the parsed ones (see
# markets_data.stream.parse): price and size already cast, time already a timestamp.
REJECT_RULES: list[tuple[str, str]] = [
    ("not a trade record", "trade_id IS NULL"),
    ("key is not the product", "product_id IS NULL OR kafka_key IS NULL OR kafka_key <> product_id"),
    ("price is not a positive decimal", "price IS NULL OR price <= 0"),
    ("size is not a positive decimal", "size IS NULL OR size <= 0"),
    ("side is not buy or sell", "side IS NULL OR side NOT IN ('buy', 'sell')"),
    ("time is not a timestamp", "trade_time IS NULL"),
]


def reject_reason_sql() -> str:
    whens = " ".join(f"WHEN {cond} THEN '{reason}'" for reason, cond in REJECT_RULES)
    return f"CASE {whens} END"


BRONZE_DDL = f"""
CREATE TABLE IF NOT EXISTS {BRONZE} (
    kafka_key string, payload string, kafka_partition int, kafka_offset bigint,
    kafka_timestamp timestamp, ingested_at timestamp
) USING iceberg PARTITIONED BY (days(kafka_timestamp))
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""

REJECTS_DDL = f"""
CREATE TABLE IF NOT EXISTS {REJECTS} (
    reason string, kafka_key string, payload string, kafka_partition int, kafka_offset bigint,
    kafka_timestamp timestamp, rejected_at timestamp
) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""

SILVER_DDL = f"""
CREATE TABLE IF NOT EXISTS {SILVER} (
    trade_id bigint, product_id string, base_currency string, quote_currency string,
    price {DECIMAL}, size {DECIMAL}, side string, trade_time timestamp, sequence bigint,
    kafka_partition int, kafka_offset bigint, ingested_at timestamp
) USING iceberg PARTITIONED BY (days(trade_time))
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""

SILVER_COLUMNS = [
    "trade_id", "product_id", "base_currency", "quote_currency", "price", "size", "side",
    "trade_time", "sequence", "kafka_partition", "kafka_offset", "ingested_at",
]  # fmt: skip
