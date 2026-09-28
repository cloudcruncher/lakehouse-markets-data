"""The card-authorisation stream's transforms on a plain local Spark session: no Kafka, Polaris or storage.

Runs inside the image (CI and `make spark-check`), where PySpark is:
  spark-submit --master local[1] tests/spark/check_card_auths.py
"""

import json
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession

from markets_data import card_stream

T = datetime(2026, 9, 28, 10, 15, 0)  # a Monday
GOOD = {
    "auth_id": "a-1",
    "card_token": "tok_100001",
    "merchant_id": "m-0002",
    "merchant_name": "Tesco Express Camden",
    "mcc": "5411",
    "merchant_country": "GB",
    "amount": 42.5,
    "currency": "GBP",
    "channel": "contactless",
    "response_code": "00",
    "auth_time": "2026-09-28T10:15:00.000Z",
}


def record(offset, key="tok_100001", **changes):
    value = changes.pop("raw", None) or json.dumps(dict(GOOD, **changes))
    return (key.encode() if key else None, value.encode(), "markets.payments.card-auths", 0, offset, T, 0)


def main():
    spark = SparkSession.builder.master("local[1]").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    fx = spark.createDataFrame(
        [
            (date(2026, 9, 25), "EUR", "GBP", 0.84),  # Friday: Monday morning's rate
            (date(2026, 9, 24), "EUR", "GBP", 0.90),
            (date(2026, 9, 25), "EUR", "JPY", 160.0),
            (date(2026, 9, 1), "EUR", "SEK", 11.0),  # too old for 28 Sep
        ],
        "rate_date date, base string, quote string, rate double",
    )
    kafka = spark.createDataFrame(
        [
            record(0),
            record(1),  # a producer retry: same auth_id
            record(2, auth_id="a-2", amount=16000, currency="JPY", response_code="51"),
            record(3, auth_id="a-3", amount=10, currency="EUR", channel="ecom"),
            # The generator's malformed records (tok_malformed), and more:
            record(
                4,
                key="tok_malformed",
                raw=json.dumps({"auth_id": "x-1", "currency": "EUR", "note": "no amount"}),
            ),
            record(
                5, key="tok_malformed", raw=json.dumps({"auth_id": "x-2", "amount": -12.5, "currency": "EUR"})
            ),
            record(
                6, key="tok_malformed", raw=json.dumps({"auth_id": "x-3", "amount": 10, "currency": "XXX"})
            ),
            record(7, key="tok_malformed", raw=json.dumps("garbled record from a legacy acquirer")),
            record(8, auth_id="a-4", currency="SEK"),
            record(9, auth_id="a-5", key="tok_999999"),
            record(10, auth_id="a-6", auth_time="yesterday"),
            record(11, auth_id="a-7", channel="atm"),
            record(12, auth_id="a-8", response_code="approved"),
            record(13, auth_id="a-9", merchant_id=None),
        ],
        "key binary, value binary, topic string, partition int, offset long, "
        "timestamp timestamp, timestampType int",
    )
    parsed = card_stream.parse(card_stream.raw(kafka), fx)
    good = {r.auth_id: r for r in card_stream.valid(parsed).collect()}
    rejects = card_stream.rejected(parsed)
    bad = {r.kafka_offset: r.reason for r in rejects.collect()}

    assert set(good) == {"a-1", "a-2", "a-3"}, good
    gbp, jpy, eur = good["a-1"], good["a-2"], good["a-3"]
    assert (gbp.fx_rate, gbp.fx_rate_date) == (Decimal("0.840000"), date(2026, 9, 25)), gbp
    assert gbp.amount_eur == Decimal("50.60"), gbp.amount_eur  # 42.50 / 0.84
    assert gbp.approved is True and jpy.approved is False
    assert jpy.amount_eur == Decimal("100.00"), jpy.amount_eur
    assert (eur.fx_rate, eur.fx_rate_date, eur.amount_eur) == (
        Decimal("1.000000"),
        date(2026, 9, 28),
        Decimal("10.00"),
    )
    assert gbp.auth_time == datetime(2026, 9, 28, 10, 15), gbp.auth_time
    assert bad == {
        4: "amount is not a positive decimal",
        5: "amount is not a positive decimal",
        6: "currency is not one the ECB fixes",
        7: "not a card authorisation",
        8: "no ECB rate in the week before",
        9: "key is not the card token",
        10: "time is not a timestamp",
        11: "channel is not pos, contactless or ecom",
        12: "response code is not two digits",
        13: "merchant is missing",
    }, bad

    letter = json.loads(card_stream.dead_letters(rejects.filter("kafka_offset = 6")).first().value)
    assert letter["reason"] == "currency is not one the ECB fixes" and letter["offset"] == 6, letter
    assert json.loads(letter["payload"])["currency"] == "XXX", letter
    print("OK   card-auths stream: 3 in euro (1 duplicate dropped), 10 rejects, right reasons")


if __name__ == "__main__":
    main()
