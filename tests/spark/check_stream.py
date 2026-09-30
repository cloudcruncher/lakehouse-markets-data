"""The trades stream's transforms on a plain local Spark session: no Kafka, Polaris or storage.

Runs inside the image (CI and `make spark-check`), where PySpark is:
  spark-submit --master local[1] tests/spark/check_stream.py
"""

import json
from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession

from markets_data import stream, trades

T = datetime(2026, 9, 27, 19, 44, 39)
GOOD = {
    "trade_id": 1099205733,
    "product_id": "BTC-USD",
    "price": "84751.67",
    "size": "0.00003146",
    "side": "buy",
    "time": "2026-09-27T19:44:39.896491Z",
    "sequence": 136853740770,
}


def record(offset, key="BTC-USD", **changes):
    value = changes.pop("raw", None) or json.dumps(dict(GOOD, **changes))
    return (key.encode() if key else None, value.encode(), "markets.coinbase.trades", 0, offset, T, 0)


def main():
    spark = SparkSession.builder.master("local[1]").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    kafka = spark.createDataFrame(
        [
            record(0),
            record(1),  # Coinbase resends the last trade after a reconnect
            record(2, trade_id=1099205734, product_id="ETH-EUR", price="2694.29", key="ETH-EUR"),
            record(3, raw="not json"),
            record(4, key="ETH-USD"),
            record(5, trade_id=7, price="-1"),
            record(6, trade_id=8, size="abc"),
            record(7, trade_id=9, side="hold"),
            record(8, trade_id=10, time="yesterday"),
        ],
        "key binary, value binary, topic string, partition int, offset long, "
        "timestamp timestamp, timestampType int",
    )
    parsed = stream.parse(stream.raw(kafka))
    good = {(r.product_id, r.trade_id): r for r in stream.valid(parsed).collect()}
    bad = {r.kafka_offset: r.reason for r in stream.rejected(parsed).collect()}

    assert set(good) == {("BTC-USD", 1099205733), ("ETH-EUR", 1099205734)}, good
    btc = good[("BTC-USD", 1099205733)]
    assert (btc.base_currency, btc.quote_currency) == ("BTC", "USD")
    assert btc.price == Decimal("84751.670000000000"), btc.price
    assert btc.size == Decimal("0.000031460000"), btc.size
    assert btc.trade_time == datetime(2026, 9, 27, 19, 44, 39, 896491), btc.trade_time
    assert bad == {
        3: "not a trade record",
        4: "key is not the product",
        5: "price is not a positive decimal",
        6: "size is not a positive decimal",
        7: "side is not buy or sell",
        8: "time is not a timestamp",
    }, bad
    v2 = {r.trade_id: r for r in stream.with_notional(stream.valid(parsed)).collect()}
    # 84751.67 * 0.00003146 has 10 fractional digits: v2 must keep them, not round at a narrower scale.
    assert v2[1099205733].notional == Decimal("2.666287538200"), v2[1099205733].notional
    assert list(stream.with_notional(stream.valid(parsed)).columns) == trades.V2_COLUMNS
    print("OK   trades stream: 2 trades (1 duplicate dropped), 6 rejects, v2 notional exact")


if __name__ == "__main__":
    main()
