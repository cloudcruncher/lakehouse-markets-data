"""Compare markets_silver.trades (live) with trades_v2 (replayed) before a switch.

Both tables are read over the same window: per partition, from the first offset the replay holds
(the topic's retention start) to the lower of the two tables' last offsets (the live stream and
the replay are never caught up to the same record). Within it they must hold the same trades
with the same values. Exits 1 on any difference, so it can gate the switch.
"""

from __future__ import annotations

import sys

from markets_data import trades
from markets_data.spark import session

WINDOW = f"""
SELECT a.kafka_partition AS p, a.lo, least(a.hi, b.hi) AS hi
FROM (SELECT kafka_partition, min(kafka_offset) AS lo, max(kafka_offset) AS hi
      FROM {trades.V2} GROUP BY 1) a
JOIN (SELECT kafka_partition, max(kafka_offset) AS hi FROM {trades.SILVER} GROUP BY 1) b
  ON a.kafka_partition = b.kafka_partition
"""


def side(table: str) -> str:
    return (
        f"SELECT t.* FROM {table} t JOIN span w "
        "ON t.kafka_partition = w.p AND t.kafka_offset BETWEEN w.lo AND w.hi"
    )


# A trade is the same when its key and its values match; offsets may differ, because the record
# kept from a reconnect's resends is whichever the batch saw first.
COMPARE = f"""
WITH span AS ({WINDOW}), live AS ({side(trades.SILVER)}), replayed AS ({side(trades.V2)})
SELECT count_if(live.trade_id IS NOT NULL AND replayed.trade_id IS NOT NULL) AS in_both,
       count_if(replayed.trade_id IS NULL) AS only_live,
       count_if(live.trade_id IS NULL) AS only_replayed,
       count_if(live.trade_id IS NOT NULL AND replayed.trade_id IS NOT NULL
                AND NOT (live.price <=> replayed.price AND live.size <=> replayed.size
                         AND live.side <=> replayed.side
                         AND live.trade_time <=> replayed.trade_time)) AS changed,
       count_if(replayed.notional IS NULL AND replayed.trade_id IS NOT NULL) AS no_notional
FROM live FULL OUTER JOIN replayed
  ON live.product_id = replayed.product_id AND live.trade_id = replayed.trade_id
"""


def differs(r) -> bool:
    return bool(r.only_live or r.only_replayed or r.changed or r.no_notional or not r.in_both)


def main() -> int:
    spark = session("markets-trades-compare")
    if not spark.sql(f"SELECT 1 FROM {trades.V2} LIMIT 1").count():
        print(f"[compare] {trades.V2} is empty: run the replay first")
        return 1
    r = spark.sql(COMPARE).first()
    window = spark.sql(f"SELECT count(*) AS n FROM ({WINDOW})").first().n
    print(f"[compare] {window} partitions compared, {trades.SILVER} vs {trades.V2}")
    for name in ("in_both", "only_live", "only_replayed", "changed", "no_notional"):
        print(f"[compare]   {name:14} {r[name]:>10}")
    if differs(r):
        print("[compare] DIFFERENT: do not switch")
        return 1
    print(f"[compare] SAME: {r.in_both} trades, identical; v2 adds notional")
    return 0


if __name__ == "__main__":
    sys.exit(main())
