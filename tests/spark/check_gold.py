"""The gold SQL on sample silver rows, on a plain local Spark session (no Polaris or storage).

Runs inside the image (CI and `make spark-check`):
  spark-submit --master local[1] tests/spark/check_gold.py
"""

from datetime import date, datetime
from decimal import Decimal as D

from pyspark.sql import SparkSession

from markets_data import gold, sanctions

SINCE = gold.since_sql("2026-09-27 00:00:00")


def main():
    spark = SparkSession.builder.master("local[1]").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    t = datetime(2026, 9, 28, 10, 15)
    spark.createDataFrame(
        [  # trade_id, product, price, size, time: two in 10:15 (same microsecond), one in 10:16
            (2, "BTC-USD", D("100"), D("1"), t.replace(second=5)),
            (1, "BTC-USD", D("90"), D("3"), t.replace(second=5)),
            (3, "BTC-USD", D("110"), D("1"), t.replace(second=40)),
            (4, "BTC-USD", D("120"), D("2"), t.replace(minute=16)),
            (5, "BTC-USD", D("1"), D("1"), datetime(2026, 9, 20)),  # before the rebuild window
        ],
        "trade_id bigint, product_id string, price decimal(30,12), size decimal(30,12), trade_time timestamp",
    ).selectExpr("*", "'BTC' AS base_currency", "'USD' AS quote_currency").createOrReplaceTempView("trades")
    candles = {r.minute: r for r in spark.sql(gold.OHLCV_SQL.format(source="trades", since=SINCE)).collect()}
    assert set(candles) == {t, t.replace(minute=16)}, candles
    c = candles[t]
    assert (c.open, c.high, c.low, c.close) == (D("90"), D("110"), D("90"), D("110")), c  # open: trade 1
    assert (c.volume, c.trades) == (D("5"), 3) and c.vwap == D("96"), c  # (90*3 + 100 + 110) / 5

    spark.createDataFrame(
        [
            (t, "GB", "GBP", "pos", True, D("10.00")),
            (t, "GB", "GBP", "pos", False, D("30.00")),
            (t, "DE", "EUR", "ecom", True, D("5.00")),
        ],
        "auth_time timestamp, merchant_country string, currency string, channel string, approved boolean, "
        "amount_eur decimal(18,2)",
    ).createOrReplaceTempView("auths")
    days = {
        (r.merchant_country, r.channel): r
        for r in spark.sql(gold.CARD_DAILY_SQL.format(source="auths", since=SINCE)).collect()
    }
    gb = days[("GB", "pos")]
    assert (gb.auth_date, gb.auths, gb.approved, gb.approval_rate) == (date(2026, 9, 28), 2, 1, 0.5), gb
    assert (gb.amount_eur, gb.approved_amount_eur, gb.avg_amount_eur) == (D("40.00"), D("10.00"), D("20.00"))

    spark.createDataFrame(
        [
            ("m-0017", "Aeroflot", "RU", 3, D("99.00"), t, t),
            ("m-0002", "Tesco Express Camden", "GB", 9, D("1"), t, t),
        ],
        "merchant_id string, merchant_name string, merchant_country string, auths bigint, "
        "amount_eur decimal(18,2), first_auth timestamp, last_auth timestamp",
    ).createOrReplaceTempView("m_raw")
    spark.createDataFrame(
        [(r.merchant_id, sanctions.normalise(r.merchant_name)) for r in spark.table("m_raw").collect()],
        "merchant_id string, name_norm string",
    ).createOrReplaceTempView("m_norm")
    spark.sql("SELECT * FROM m_raw JOIN m_norm USING (merchant_id)").createOrReplaceTempView("merchants")
    spark.createDataFrame(
        [("NK-LVyj", "Organization", "aeroflot", "ru", "GB-RUS", date(2026, 9, 28))],
        "target_id string, schema string, name_norm string, countries string, program_ids string, "
        "list_date date",
    ).createOrReplaceTempView("names")
    hits = spark.sql(gold.HITS_SQL.format(merchants="merchants", names="names")).collect()
    assert [(h.merchant_id, h.target_id, h.program_ids, h.auths) for h in hits] == [
        ("m-0017", "NK-LVyj", "GB-RUS", 3)
    ], hits
    print(
        "OK   gold: candles (open/close by time then id, VWAP), daily approval and euro sums, 1 sanctions hit"
    )


if __name__ == "__main__":
    main()
