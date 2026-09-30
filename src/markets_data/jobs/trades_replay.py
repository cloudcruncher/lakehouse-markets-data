"""Kappa replay: rebuild silver from the topic into markets_silver.trades_v2 with new logic.

Kafka is the source of truth, so a change to silver's logic (here: a `notional` column) is a new
table built from the topic's retained records, not an ALTER and a backfill. The live stream keeps
writing markets_silver.trades the whole time; nothing a consumer reads changes until the view is
switched (markets_data.jobs.trades_swap), after `trades_compare` shows the two agree.

A bounded run (availableNow) with its own checkpoint under CHECKPOINTS: run it again and it
catches up from where it stopped, through the same exactly-once MERGE the live stream uses.
RESET=1 drops trades_v2 and the checkpoint first, to rebuild from the topic's first record.
"""

from __future__ import annotations

import os
import shutil

from markets_data import stream, trades
from markets_data.jobs.trades_stream import merge_trades
from markets_data.spark import session
from markets_data.streaming import CHECKPOINTS, kafka, watch

CHECKPOINT = f"{CHECKPOINTS}/trades_silver_v2"


def build_v2(batch, batch_id: int) -> None:
    parsed = stream.parse(stream.raw(batch)).persist()
    good = stream.with_notional(stream.valid(parsed))
    merge_trades(good, trades.V2)
    print(f"[replay] batch {batch_id}: {good.count()} trades -> {trades.V2}", flush=True)
    parsed.unpersist()


def main() -> None:
    spark = session("markets-trades-replay")
    if os.environ.get("RESET") == "1":
        spark.sql(f"DROP TABLE IF EXISTS {trades.V2}")
        shutil.rmtree(CHECKPOINT, ignore_errors=True)
        print(f"[replay] reset: {trades.V2} and its checkpoint dropped", flush=True)
    spark.sql(trades.V2_DDL)
    query = (
        kafka(spark, trades.TOPIC, "earliest")  # ignored once the checkpoint exists
        .writeStream.queryName("trades_silver_v2")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .foreachBatch(build_v2)
        .start()
    )
    watch(query)
    spark.catalog.refreshTable(trades.V2)  # foreachBatch wrote through a cloned session
    rows = spark.table(trades.V2).count()
    print(
        f"[replay] {trades.V2} holds {rows} trades; compare it with {trades.SILVER} before switching",
        flush=True,
    )


if __name__ == "__main__":
    main()
