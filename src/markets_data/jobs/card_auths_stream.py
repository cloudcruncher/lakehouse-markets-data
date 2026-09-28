"""The Kappa stream for card authorisations (run by spark-submit, as this tenant, for as long as it runs).

Two queries read markets.payments.card-auths, each with its own checkpoint under CHECKPOINTS
(the service's /state volume, so a restart resumes where it stopped):

  * bronze: every record as it arrived, appended (Iceberg commits are exactly once per batch)
  * silver: typed, checked and converted to euro at the ECB rate of the day
    (markets_bronze.fx_rates); valid authorisations MERGEd once by auth_id, the rest MERGEd into
    markets_bronze.card_auths_rejects with a reason and sent to the DLQ topic.

The rejects table is exactly once (MERGE by offset); the DLQ topic is at least once, since a
retried batch sends its dead letters again. Consumers of the DLQ key on (partition, offset).

Replay (the Kappa part): a new table and checkpoint rebuild from the topic, which keeps 7 days.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from markets_data import card_auths, card_stream
from markets_data.spark import session
from markets_data.streaming import BOOTSTRAP, CHECKPOINTS, TRIGGER, bronze_start, kafka, watch


def merge_silver(batch: DataFrame, batch_id: int) -> None:
    spark = batch.sparkSession
    parsed = card_stream.parse(card_stream.raw(batch), spark.table(card_auths.FX)).persist()
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


def main() -> None:
    spark = session("markets-card-auths-stream")
    for ddl in (card_auths.BRONZE_DDL, card_auths.REJECTS_DDL, card_auths.SILVER_DDL):
        spark.sql(ddl)

    checkpoint = f"{CHECKPOINTS}/card_auths_bronze"
    start = bronze_start(spark, checkpoint, card_auths.TOPIC, card_auths.BRONZE)
    bronze = (
        card_stream.raw(kafka(spark, card_auths.TOPIC, start))
        .writeStream.queryName("card_auths_bronze")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=TRIGGER)
        .toTable(card_auths.BRONZE)
    )
    silver = (
        kafka(spark, card_auths.TOPIC)
        .writeStream.queryName("card_auths_silver")
        .option("checkpointLocation", f"{CHECKPOINTS}/card_auths_silver")
        .trigger(processingTime=TRIGGER)
        .foreachBatch(merge_silver)
        .start()
    )
    print(
        f"[card-auths] streaming {card_auths.TOPIC} -> {card_auths.BRONZE}, {card_auths.SILVER}", flush=True
    )
    watch(bronze, silver)


if __name__ == "__main__":
    main()
