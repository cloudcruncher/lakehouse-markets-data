"""Idempotent bronze: every Kafka record once, whatever the checkpoint says.

A plain append made bronze exactly as reliable as its checkpoint: a stale checkpoint (one that lags the
table, as after a volume restored from an older state) re-read offsets the table already held and
appended them again, and nothing noticed. The platform's lag metric did (tenant_table_lag_records went
negative). Bronze now MERGEs each batch on (kafka_partition, kafka_offset), the record's identity in Kafka,
so replays, retries and restored checkpoints add nothing twice.

No Spark here, so the SQL is unit-tested; markets_data.streaming runs it.
"""

from __future__ import annotations

from datetime import datetime


def merge_sql(table: str, oldest: datetime, min_offset: int) -> str:
    """MERGE the `bronze_batch` view into a bronze table, inserting only records it does not hold.

    The two extra predicates only narrow what is scanned: the day partition of the batch's oldest
    record (bronze is partitioned by days(kafka_timestamp)) and the lowest offset in the batch (files
    hold offsets in ranges, so the rest are skipped by their statistics). They never change the result:
    a record the batch holds cannot be older or lower than its own minimum.
    """
    return f"""
        MERGE INTO {table} t USING bronze_batch s
        ON t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset
           AND t.kafka_timestamp >= TIMESTAMP '{oldest:%Y-%m-%d}' AND t.kafka_offset >= {min_offset}
        WHEN NOT MATCHED THEN INSERT *
    """
