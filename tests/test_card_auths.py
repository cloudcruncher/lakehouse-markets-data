"""The ShadowTraffic config: what it sends, and that the stream will be able to convert it."""

import json
from pathlib import Path

CONFIG = json.loads((Path(__file__).parents[1] / "generators/card-auths/card-auths.json").read_text())
GOOD, BAD = CONFIG["generators"]
# Currencies with an ECB reference rate (markets_bronze.fx_rates), plus the base.
ECB = {"EUR", "USD", "JPY", "GBP", "CHF", "SEK", "PLN", "CZK", "DKK", "HUF", "NOK", "AUD", "CAD"}


def test_both_generators_write_the_tenants_topic():
    assert GOOD["topic"] == BAD["topic"] == "markets.payments.card-auths"


def test_every_currency_converts_to_eur():
    currencies = {c["value"]["currency"] for c in GOOD["vars"]["money"]["choices"]}
    assert currencies <= ECB, currencies - ECB


def test_jpy_has_no_minor_units():
    jpy = next(c for c in GOOD["vars"]["money"]["choices"] if c["value"]["currency"] == "JPY")
    assert jpy["value"]["amount"]["decimals"] == 0


def test_keyed_by_card_so_a_cards_authorisations_stay_in_order():
    assert GOOD["key"] == GOOD["value"]["card_token"]


def test_malformed_records_are_about_one_percent():
    good = sum(GOOD["localConfigs"]["throttleMs"]["bounds"]) / 2
    bad = sum(BAD["localConfigs"]["throttleMs"]["bounds"]) / 2
    assert 0.002 < good / bad < 0.02


def test_kafka_comes_from_the_platform_not_the_file():
    assert CONFIG["connections"]["kafka"]["producerConfigs"]["bootstrap.servers"] == {
        "_gen": "env",
        "var": "KAFKA_BOOTSTRAP",
    }
