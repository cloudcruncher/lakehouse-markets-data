"""Card authorisations in the lakehouse: table names, layouts and the rules an authorisation must pass.

No Spark here, so the rules are unit-tested in seconds; the stream (markets_data.card_stream)
applies them as SQL. Kappa, as for trades: bronze keeps every record as it arrived, silver keeps
each valid authorisation once, with its amount in euro at the ECB rate of its day.
"""

TOPIC = "markets.payments.card-auths"
DLQ_TOPIC = "markets.payments.card-auths.dlq"
BRONZE = "markets_bronze.card_auths"
REJECTS = "markets_bronze.card_auths_rejects"
SILVER = "markets_silver.card_auths"
FX = "markets_bronze.fx_rates"

AMOUNT = "decimal(18,2)"
RATE = "decimal(18,6)"
# The ECB fixes no rate at weekends or on TARGET holidays (Easter is four days): an authorisation
# takes the latest fixing on or before its day, at most this old. Older means the FX job is
# stuck, and converting at a stale rate would hide that.
FX_MAX_AGE_DAYS = 7
CHANNELS = ("pos", "contactless", "ecom")

VALUE_SCHEMA = (
    "auth_id string, card_token string, merchant_id string, merchant_name string, mcc string, "
    f"merchant_country string, amount {AMOUNT}, currency string, channel string, "
    "response_code string, auth_time string"
)

# First match wins; an authorisation that matches none is valid. Columns are the parsed ones
# (markets_data.card_stream): amount already a decimal, auth_time a timestamp, fx_rate joined, and
# currency_known true for euro and every currency the ECB has ever fixed.
REJECT_RULES: list[tuple[str, str]] = [
    ("not a card authorisation", "auth_id IS NULL"),
    ("amount is not a positive decimal", "amount IS NULL OR amount <= 0"),
    ("currency is not one the ECB fixes", "NOT currency_known"),
    ("time is not a timestamp", "auth_time IS NULL"),
    # A known currency without a recent rate: the FX job is behind (replay the reject once it runs).
    ("no ECB rate in the week before", "fx_rate IS NULL"),
    ("key is not the card token", "card_token IS NULL OR kafka_key IS NULL OR kafka_key <> card_token"),
    ("merchant is missing", "merchant_id IS NULL OR merchant_country IS NULL"),
    ("channel is not pos, contactless or ecom", f"channel IS NULL OR channel NOT IN {CHANNELS}"),
    ("response code is not two digits", "response_code IS NULL OR NOT response_code RLIKE '^[0-9]{2}$'"),
]


def reject_reason_sql() -> str:
    whens = " ".join(f"WHEN {cond} THEN '{reason}'" for reason, cond in REJECT_RULES)
    return f"CASE {whens} END"


BRONZE_DDL = f"""
CREATE TABLE IF NOT EXISTS {BRONZE} (
    kafka_key string, payload string, kafka_partition int, kafka_offset bigint,
    kafka_timestamp timestamp, ingested_at timestamp
) USING iceberg PARTITIONED BY (days(kafka_timestamp))
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""

REJECTS_DDL = f"""
CREATE TABLE IF NOT EXISTS {REJECTS} (
    reason string, kafka_key string, payload string, kafka_partition int, kafka_offset bigint,
    kafka_timestamp timestamp, rejected_at timestamp
) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""

SILVER_DDL = f"""
CREATE TABLE IF NOT EXISTS {SILVER} (
    auth_id string, card_token string, merchant_id string, merchant_name string, mcc string,
    merchant_country string, channel string, response_code string, approved boolean,
    amount {AMOUNT}, currency string, fx_rate {RATE}, fx_rate_date date, amount_eur {AMOUNT},
    auth_time timestamp, kafka_partition int, kafka_offset bigint, ingested_at timestamp
) USING iceberg PARTITIONED BY (days(auth_time))
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""

SILVER_COLUMNS = [
    "auth_id", "card_token", "merchant_id", "merchant_name", "mcc", "merchant_country", "channel",
    "response_code", "approved", "amount", "currency", "fx_rate", "fx_rate_date", "amount_eur",
    "auth_time", "kafka_partition", "kafka_offset", "ingested_at",
]  # fmt: skip
