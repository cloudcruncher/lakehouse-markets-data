from datetime import datetime

from markets_data import bronze


def test_bronze_is_merged_on_the_records_identity_in_kafka_and_only_inserts():
    sql = bronze.merge_sql("markets_bronze.trades", datetime(2026, 9, 28, 21, 15, 3), 120)
    assert "MERGE INTO markets_bronze.trades t USING bronze_batch s" in sql
    assert "t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql
    assert "WHEN MATCHED" not in sql  # a record already held is never rewritten


def test_the_scan_is_narrowed_to_the_batchs_day_and_lowest_offset():
    sql = bronze.merge_sql("b.t", datetime(2026, 9, 28, 21, 15, 3), 120)
    assert "t.kafka_timestamp >= TIMESTAMP '2026-09-28'" in sql
    assert "t.kafka_offset >= 120" in sql
