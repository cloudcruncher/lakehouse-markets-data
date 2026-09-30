"""The replay comparison on sample tables: it must count each kind of difference, and only inside
the window both tables cover.

Runs inside the image (CI and `make spark-check`):
  spark-submit --master local[1] tests/spark/check_compare.py
"""

from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession

from markets_data import trades
from markets_data.jobs import trades_compare as compare

T = datetime(2026, 9, 27, 19, 44, 39)
COLS = (
    "trade_id bigint, product_id string, price decimal(30,12), size decimal(30,12), side string, "
    "trade_time timestamp, kafka_partition int, kafka_offset bigint"
)


def row(trade_id, offset, price=100, side="buy"):
    return (trade_id, "BTC-USD", Decimal(price), Decimal(1), side, T, 0, offset)


def tables(spark, live, replayed):
    spark.createDataFrame(live, COLS).write.mode("overwrite").saveAsTable(trades.SILVER)
    v2 = [(*r, r[2] * r[3]) for r in replayed]
    spark.createDataFrame(v2, COLS + ", notional decimal(30,12)").write.mode("overwrite").saveAsTable(
        trades.V2
    )


def main():
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", "/tmp/warehouse")  # the image's working directory is read-only
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("CREATE DATABASE IF NOT EXISTS markets_silver")

    same = [row(i, i) for i in range(10, 20)]
    tables(spark, same, same)
    r = spark.sql(compare.COMPARE).first()
    assert (r.in_both, r.only_live, r.only_replayed, r.changed) == (10, 0, 0, 0), r
    assert not compare.differs(r)

    # Offsets 0-9 are before the replay's first record (retention deleted them): not compared.
    # The live table is ahead at offset 30 (the replay has not caught up): not compared either.
    live = [row(i, i) for i in range(0, 10)] + [row(10, 10), row(11, 11), row(12, 12, price=100), row(30, 30)]
    replayed = [row(11, 11), row(12, 12, price=101), row(13, 13), row(14, 14, side="sell")]
    tables(spark, live, replayed)
    r = spark.sql(compare.COMPARE).first()
    # window: offsets 11..14 (v2's first offset to the lower last offset); 14 is only in v2.
    assert (r.in_both, r.only_live, r.only_replayed, r.changed) == (2, 0, 2, 1), r
    assert compare.differs(r)

    # A trade the replay lost inside the window.
    live = [row(i, i) for i in range(10, 16)]
    replayed = [row(i, i) for i in range(10, 16) if i != 12]
    tables(spark, live, replayed)
    r = spark.sql(compare.COMPARE).first()
    assert (r.in_both, r.only_live, r.only_replayed, r.changed) == (5, 1, 0, 0), r
    assert compare.differs(r)
    print("OK   replay compare: same, changed, only-live and only-replayed are told apart; window honoured")


if __name__ == "__main__":
    main()
