"""The Kappa stream for card authorisations (spark-submit, as this tenant; markets_data.scale says when).

Two queries read markets.payments.card-auths, each with its own checkpoint under CHECKPOINTS
(the service's /state volume, so a restart resumes where it stopped):

  * bronze: every record as it arrived, MERGEd on (partition, offset) so a stale checkpoint adds nothing twice
  * silver: typed, checked and converted to euro at the ECB rate of the day
    (markets_bronze.fx_rates); valid authorisations MERGEd once by auth_id, the rest MERGEd into
    markets_bronze.card_auths_rejects with a reason and sent to the DLQ topic.

The rejects table is exactly once (MERGE by offset); the DLQ topic is at least once, since a
retried batch sends its dead letters again. Consumers of the DLQ key on (partition, offset).

Replay (the Kappa part): a new table and checkpoint rebuild from the topic, which keeps 7 days.
"""

from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from markets_data import card_auths, card_stream
from markets_data.scale import trigger_kwargs
from markets_data.spark import session
from markets_data.streaming import BOOTSTRAP, CHECKPOINTS, kafka, merge_bronze, resume_start, watch

# The ECB fixes once a business day: reading markets_bronze.fx_rates from object storage every
# batch only loads the store. The rates stay cached in the driver's memory and refresh hourly.
FX_REFRESH_SECONDS = 3600
_fx: dict = {"rates": None, "loaded": 0.0}


def fx_rates(spark: SparkSession) -> DataFrame:
    if _fx["rates"] is None or time.monotonic() - _fx["loaded"] > FX_REFRESH_SECONDS:
        if _fx["rates"] is not None:
            _fx["rates"].unpersist()
        _fx["rates"] = card_stream.eur_rates(spark.table(card_auths.FX)).cache()
        n = _fx["rates"].count()  # materialise the cache now, not in the middle of a join
        _fx["loaded"] = time.monotonic()
        print(f"[card-auths] {n} ECB rates cached", flush=True)
    return _fx["rates"]


def merge_silver(batch: DataFrame, batch_id: int) -> None:
    spark = batch.sparkSession
    parsed = card_stream.parse(card_stream.raw(batch), fx_rates(spark)).persist()
    good, bad = card_stream.valid(parsed), card_stream.rejected(parsed).persist()
    good.createOrReplaceTempView("card_auths_batch")
    bad.createOrReplaceTempView("card_auth_rejects_batch")
    oldest = good.agg(F.min("auth_time")).first()[0]
    if oldest is not None:
        # Only the days this batch touches (the table is partitioned by day of auth_time).
        spark.sql(f"""
            MERGE INTO {card_auths.SILVER} t USING card_auths_batch s
            ON t.auth_id = s.auth_id AND t.auth_time >= TIMESTAMP '{oldest:%Y-%m-%d}'
            WHEN NOT MATCHED THEN INSERT *
        """)
    n_bad = bad.count()
    if n_bad:
        spark.sql(f"""
            MERGE INTO {card_auths.REJECTS} t USING card_auth_rejects_batch s
            ON t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset
            WHEN NOT MATCHED THEN INSERT *
        """)
        (
            card_stream.dead_letters(bad)
            .write.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("topic", card_auths.DLQ_TOPIC)
            .save()
        )
    print(f"[card-auths] batch {batch_id}: {good.count()} authorisations, {n_bad} rejected", flush=True)
    bad.unpersist()
    parsed.unpersist()


def start(spark: SparkSession) -> list[StreamingQuery]:
    """Create the tables if needed and start both queries (bronze, silver)."""
    for ddl in (card_auths.BRONZE_DDL, card_auths.REJECTS_DDL, card_auths.SILVER_DDL):
        spark.sql(ddl)
    bronze_cp, silver_cp = f"{CHECKPOINTS}/card_auths_bronze", f"{CHECKPOINTS}/card_auths_silver"
    bronze = (
        kafka(spark, card_auths.TOPIC, resume_start(spark, bronze_cp, card_auths.TOPIC, [card_auths.BRONZE]))
        .writeStream.queryName("card_auths_bronze")
        .option("checkpointLocation", bronze_cp)
        .trigger(**trigger_kwargs())
        .foreachBatch(merge_bronze(card_auths.BRONZE, card_stream.raw))
        .start()
    )
    silver_start = resume_start(spark, silver_cp, card_auths.TOPIC, [card_auths.SILVER, card_auths.REJECTS])
    silver = (
        kafka(spark, card_auths.TOPIC, silver_start)
        .writeStream.queryName("card_auths_silver")
        .option("checkpointLocation", silver_cp)
        .trigger(**trigger_kwargs())
        .foreachBatch(merge_silver)
        .start()
    )
    print(
        f"[card-auths] streaming {card_auths.TOPIC} -> {card_auths.BRONZE}, {card_auths.SILVER}", flush=True
    )
    return [bronze, silver]


def main() -> None:
    watch(*start(session("markets-card-auths-stream")))


if __name__ == "__main__":
    main()
