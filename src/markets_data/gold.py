"""The gold data products: what they hold, how they're built from silver, and the checks each
build must pass.

No Spark session here, only SQL text and pure Python, so the layouts and checks are unit-tested
in seconds; tests/spark runs the SQL on sample rows. markets_gold is readable by every colleague
(the platform's entitlements), so nothing here carries a card token or a person.
"""

import os

from markets_data import card_auths, sanctions, trades

OHLCV = "markets_gold.crypto_ohlcv_1m"
CARD_DAILY = "markets_gold.card_auth_daily"
HITS = "markets_gold.sanctions_hits"

# Each hourly build recomputes the days it may have missed or that may still change: yesterday
# (late records, a stream catching up) and today. Older days are final.
# GOLD_REBUILD_DAYS widens one run to backfill after a logic fix; the window is replaced, so it is repeatable.
REBUILD_DAYS = int(os.environ.get("GOLD_REBUILD_DAYS", "2"))

OHLCV_DDL = f"""
CREATE TABLE IF NOT EXISTS {OHLCV} (
    product_id string, base_currency string, quote_currency string, minute timestamp,
    open {trades.DECIMAL}, high {trades.DECIMAL}, low {trades.DECIMAL}, close {trades.DECIMAL},
    volume {trades.DECIMAL}, notional {trades.DECIMAL}, vwap {trades.DECIMAL}, trades bigint,
    built_at timestamp
) USING iceberg PARTITIONED BY (days(minute))
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""

# Open and close are the first and last trade of the minute by (trade_time, trade_id): Coinbase
# can stamp several trades with the same microsecond.
#
# price * size of two decimal(30,12) would overflow Spark's 38 digits, and Spark answers by dropping
# fractional digits: a minute that traded at one price got a VWAP a hair outside [low, high] and failed
# candles_are_consistent. Narrowing the operands first keeps every digit (trades.NOTIONAL_SQL, for
# silver). A VWAP is a weighted average of prices, so it is held inside [low, high] by definition; the
# division is done in double, whose 15 digits are plenty for a price, then cast back.
VALUE = "CAST(price AS decimal(20,12)) * CAST(size AS decimal(20,12))"
AVERAGE = f"CAST(sum({VALUE}) AS double) / CAST(sum(size) AS double)"
LOW, HIGH = "CAST(min(price) AS double)", "CAST(max(price) AS double)"
VWAP = f"CAST(least(greatest({AVERAGE}, {LOW}), {HIGH}) AS {trades.DECIMAL})"
OHLCV_SQL = f"""
SELECT product_id, first(base_currency) AS base_currency, first(quote_currency) AS quote_currency,
       date_trunc('MINUTE', trade_time) AS minute,
       min_by(price, struct(trade_time, trade_id)) AS open, max(price) AS high, min(price) AS low,
       max_by(price, struct(trade_time, trade_id)) AS close,
       sum(size) AS volume,
       CAST(sum({VALUE}) AS {trades.DECIMAL}) AS notional,
       {VWAP} AS vwap,
       count(*) AS trades, current_timestamp() AS built_at
FROM {{source}}
WHERE trade_time >= {{since}}
GROUP BY product_id, date_trunc('MINUTE', trade_time)
"""

CARD_DAILY_DDL = f"""
CREATE TABLE IF NOT EXISTS {CARD_DAILY} (
    auth_date date, merchant_country string, currency string, channel string,
    auths bigint, approved bigint, approval_rate double,
    amount_eur {card_auths.AMOUNT}, approved_amount_eur {card_auths.AMOUNT},
    avg_amount_eur {card_auths.AMOUNT},
    built_at timestamp
) USING iceberg PARTITIONED BY (auth_date)
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""

CARD_DAILY_SQL = f"""
SELECT to_date(auth_time) AS auth_date, merchant_country, currency, channel,
       count(*) AS auths, count_if(approved) AS approved,
       round(count_if(approved) / count(*), 4) AS approval_rate,
       CAST(sum(amount_eur) AS {card_auths.AMOUNT}) AS amount_eur,
       CAST(coalesce(sum(CASE WHEN approved THEN amount_eur END), 0) AS {card_auths.AMOUNT})
           AS approved_amount_eur,
       CAST(avg(amount_eur) AS {card_auths.AMOUNT}) AS avg_amount_eur,
       current_timestamp() AS built_at
FROM {{source}}
WHERE auth_time >= {{since}}
GROUP BY to_date(auth_time), merchant_country, currency, channel
"""

HITS_DDL = f"""
CREATE TABLE IF NOT EXISTS {HITS} (
    merchant_id string, merchant_name string, merchant_country string, name_norm string,
    target_id string, target_schema string, program_ids string,
    auths bigint, amount_eur {card_auths.AMOUNT}, first_auth timestamp, last_auth timestamp,
    list_date date, screened_at timestamp
) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""

# Merchants with their normalised name (a view built in the driver with sanctions.normalise, so
# both sides of the screen use the same function) against today's listed names.
HITS_SQL = """
SELECT m.merchant_id, m.merchant_name, m.merchant_country, m.name_norm,
       n.target_id, n.schema AS target_schema, n.program_ids,
       m.auths, m.amount_eur, m.first_auth, m.last_auth, n.list_date,
       current_timestamp() AS screened_at
FROM {merchants} m JOIN {names} n ON m.name_norm = n.name_norm
"""

MERCHANTS_SQL = f"""
SELECT merchant_id, first(merchant_name) AS merchant_name, first(merchant_country) AS merchant_country,
       count(*) AS auths, CAST(sum(amount_eur) AS {card_auths.AMOUNT}) AS amount_eur,
       min(auth_time) AS first_auth, max(auth_time) AS last_auth
FROM {card_auths.SILVER}
GROUP BY merchant_id
"""

# (check name, asset, SQL returning the number of offending rows): each must return 0.
CHECKS: list[tuple[str, str, str]] = [
    (
        "candles_are_consistent",
        OHLCV,
        f"SELECT count(*) FROM {OHLCV} WHERE NOT (low <= open AND open <= high AND low <= close "
        "AND close <= high AND low <= vwap AND vwap <= high AND volume > 0 AND trades > 0)",
    ),
    (
        "one_candle_per_product_minute",
        OHLCV,
        f"SELECT count(*) FROM (SELECT 1 FROM {OHLCV} GROUP BY product_id, minute HAVING count(*) > 1)",
    ),
    (
        "approval_rate_is_a_share",
        CARD_DAILY,
        f"SELECT count(*) FROM {CARD_DAILY} WHERE approval_rate < 0 OR approval_rate > 1 OR approved > auths",
    ),
    (
        "daily_counts_match_silver",
        CARD_DAILY,
        # For the days this build rebuilt: gold must count every silver authorisation once.
        f"SELECT abs((SELECT coalesce(sum(auths), 0) FROM {CARD_DAILY} WHERE auth_date >= {{since_date}})"
        f" - (SELECT count(*) FROM {card_auths.SILVER} WHERE auth_time >= {{since}}))",
    ),
    (
        "hits_are_on_todays_list",
        HITS,
        f"SELECT count(*) FROM {HITS} h LEFT JOIN {sanctions.BRONZE} t ON h.target_id = t.target_id "
        "WHERE t.target_id IS NULL OR t.delisted_at IS NOT NULL",
    ),
]


def since_sql(rebuild_from: str) -> str:
    return f"TIMESTAMP '{rebuild_from}'"
