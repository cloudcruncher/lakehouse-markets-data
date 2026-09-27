import json

import pytest

from markets_data import coinbase

# As the feed sends them (captured 27 Sep 2026), order ids included.
MATCH = {
    "type": "match",
    "trade_id": 1099205733,
    "maker_order_id": "5ea839da-b609-4c9d-b7aa-142266061cdb",
    "taker_order_id": "fdc5f1ca-37a8-4fd0-b1d2-dad01d2bc1cc",
    "side": "buy",
    "size": "0.00003146",
    "price": "84751.67",
    "product_id": "BTC-USD",
    "sequence": 136853740770,
    "time": "2026-09-27T19:44:39.896491Z",
}


def test_a_trade_is_keyed_by_product():
    key, value = coinbase.to_record(MATCH)
    assert key == "BTC-USD"
    assert json.loads(value) == {
        "trade_id": 1099205733,
        "product_id": "BTC-USD",
        "price": "84751.67",
        "size": "0.00003146",
        "side": "buy",
        "time": "2026-09-27T19:44:39.896491Z",
        "sequence": 136853740770,
    }


def test_prices_stay_strings_so_no_decimal_is_lost():
    _, value = coinbase.to_record(dict(MATCH, price="0.000000012345678901"))
    assert json.loads(value)["price"] == "0.000000012345678901"


def test_last_match_is_sent_too_silver_dedupes_it():
    assert coinbase.to_record(dict(MATCH, type="last_match"))[0] == "BTC-USD"


@pytest.mark.parametrize("kind", ["subscriptions", "heartbeat", "ticker"])
def test_non_trades_are_skipped(kind):
    assert coinbase.to_record({"type": kind}) is None


def test_a_refused_subscription_stops_the_producer():
    with pytest.raises(coinbase.FeedError, match="Failed to subscribe: FOO-BAR is not a valid product"):
        coinbase.to_record(
            {"type": "error", "message": "Failed to subscribe", "reason": "FOO-BAR is not a valid product"}
        )


def test_subscribe_asks_for_matches_only():
    assert json.loads(coinbase.subscribe(("BTC-USD", "ETH-EUR"))) == {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-EUR"],
        "channels": ["matches"],
    }
