"""The ShadowTraffic config: what it sends, and that the stream will be able to convert it."""

import json
from pathlib import Path

from markets_data import card_auths
from markets_data.submit import JOBS

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


# The stream's rules and layouts (markets_data.card_auths); tests/spark checks them on records.
def test_every_rule_has_a_distinct_reason():
    reasons = [r for r, _ in card_auths.REJECT_RULES]
    assert len(reasons) == len(set(reasons))


def test_a_malformed_amount_is_named_before_the_missing_card_token():
    # The generator's malformed records have no card_token; the reason should say what's wrong.
    reasons = [r for r, _ in card_auths.REJECT_RULES]
    assert reasons.index("amount is not a positive decimal") < reasons.index("key is not the card token")
    assert reasons.index("currency is not one the ECB fixes") < reasons.index("key is not the card token")


def test_channel_rule_matches_the_generator():
    channels = {c["value"] for c in GOOD["value"]["channel"]["choices"]}
    assert channels == set(card_auths.CHANNELS)
    assert "channel NOT IN ('pos', 'contactless', 'ecom')" in card_auths.reject_reason_sql()


def test_silver_ddl_has_the_silver_columns_in_order():
    body = card_auths.SILVER_DDL.split("(", 1)[1].split(") USING", 1)[0]
    cols = [c.split()[0] for c in body.replace("\n", " ").split(", ")]
    assert cols == card_auths.SILVER_COLUMNS


def test_the_stream_writes_the_tenants_topics():
    assert card_auths.TOPIC == GOOD["topic"]
    assert card_auths.DLQ_TOPIC == f"{card_auths.TOPIC}.dlq"


def test_the_stream_job_is_shipped():
    assert (JOBS / "card_auths_stream.py").is_file()


def test_one_merchant_is_on_the_uk_sanctions_list_and_rare():
    # "Aeroflot" is PJSC Aeroflot on the FCDO list: screening (markets_gold.sanctions_hits) must find it.
    choices = GOOD["vars"]["merchant"]["choices"]
    listed = [c for c in choices if c["value"]["merchant_name"] == "Aeroflot"]
    assert len(listed) == 1 and listed[0]["value"]["merchant_country"] == "RU"
    share = listed[0]["weight"] / sum(c["weight"] for c in choices)
    assert share < 0.01
