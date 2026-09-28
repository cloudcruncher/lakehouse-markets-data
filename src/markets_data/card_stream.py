"""Spark transforms for the card-authorisation stream: Kafka records -> typed authorisations in
euro, each with a reject reason.

Kept apart from the job so a plain local Spark session can check them (tests/spark), without
Kafka, Polaris or storage. Bronze rows and rejects have the same layout as the trades stream's.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from markets_data import card_auths
from markets_data.stream import raw, rejected

__all__ = ["raw", "parse", "valid", "rejected", "dead_letters"]


def eur_rates(fx: DataFrame) -> DataFrame:
    """ECB euro rates (markets_bronze.fx_rates) as (fx_quote, fx_rate_date, fx_rate)."""
    return fx.filter("base = 'EUR'").select(
        F.col("quote").alias("fx_quote"),
        F.col("rate_date").alias("fx_rate_date"),
        F.col("rate").cast(card_auths.RATE).alias("fx_rate"),
    )


def parse(bronze: DataFrame, fx: DataFrame) -> DataFrame:
    """Typed authorisations in euro, each with `reject_reason` (NULL when valid).

    Each takes the latest ECB fixing on or before its day (at most FX_MAX_AGE_DAYS old); euro
    itself converts at 1.
    """
    a = F.from_json("payload", card_auths.VALUE_SCHEMA)
    typed = bronze.select(
        "kafka_key",
        "payload",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingested_at",
        *(a[c].alias(c) for c in ("auth_id", "card_token", "merchant_id", "merchant_name", "mcc")),
        *(a[c].alias(c) for c in ("merchant_country", "amount", "currency", "channel", "response_code")),
        a["auth_time"].cast("timestamp").alias("auth_time"),
    ).withColumn("auth_date", F.to_date("auth_time"))

    rates = F.broadcast(eur_rates(fx))
    known = F.broadcast(rates.select(F.col("fx_quote").alias("known_quote")).distinct())
    typed = typed.join(known, typed.currency == known.known_quote, "left").select(
        typed["*"], (F.col("known_quote").isNotNull() | (F.col("currency") == "EUR")).alias("currency_known")
    )
    joined = typed.join(
        rates,
        (typed.currency == rates.fx_quote)
        & (rates.fx_rate_date <= typed.auth_date)
        & (rates.fx_rate_date > F.date_sub(typed.auth_date, card_auths.FX_MAX_AGE_DAYS)),
        "left",
    )
    latest = Window.partitionBy("kafka_partition", "kafka_offset").orderBy(F.desc_nulls_last("fx_rate_date"))
    is_eur = F.col("currency") == "EUR"
    return (
        joined.withColumn("_n", F.row_number().over(latest))
        .filter("_n = 1")
        .drop("_n", "fx_quote")
        .withColumn("fx_rate", F.when(is_eur, F.lit(1).cast(card_auths.RATE)).otherwise(F.col("fx_rate")))
        .withColumn("fx_rate_date", F.when(is_eur, F.col("auth_date")).otherwise(F.col("fx_rate_date")))
        .withColumn("amount_eur", F.round(F.col("amount") / F.col("fx_rate"), 2).cast(card_auths.AMOUNT))
        .withColumn("approved", F.col("response_code") == "00")
        .withColumn("reject_reason", F.expr(card_auths.reject_reason_sql()))
    )


def valid(parsed: DataFrame) -> DataFrame:
    """Each valid authorisation once (a producer retry resends the same auth_id)."""
    return (
        parsed.filter("reject_reason IS NULL").dropDuplicates(["auth_id"]).select(*card_auths.SILVER_COLUMNS)
    )


def dead_letters(rejects: DataFrame) -> DataFrame:
    """Rejects as Kafka records for the DLQ topic: the original key, and a JSON value saying why
    and where the record came from, so a consumer can find it in bronze."""
    return rejects.select(
        F.col("kafka_key").alias("key"),
        F.to_json(
            F.struct(
                "reason",
                F.lit(card_auths.TOPIC).alias("topic"),
                F.col("kafka_partition").alias("partition"),
                F.col("kafka_offset").alias("offset"),
                "payload",
            )
        ).alias("value"),
    )
