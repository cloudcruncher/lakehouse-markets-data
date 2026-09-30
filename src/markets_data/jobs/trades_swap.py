"""Exchange the live silver table with the replayed one by renaming, so nothing else changes.

TO=v2: markets_silver.trades -> trades_v1, then trades_v2 -> trades, only if the comparison finds
them identical (FORCE=1 skips that, for a replay that is meant to differ). TO=v1 is the rollback:
trades -> trades_v2, trades_v1 -> trades. Table names carry the version, so the state is always
readable from them, and no data moves: a rename is a catalog operation, the same in Spark, Trino
and every reader. Readers and the stream keep using `markets_silver.trades`.

Between the two renames the name does not exist for a moment: a stream batch landing then fails
and its service restarts from the checkpoint, losing nothing (the catalog cache is off, so the
next batch sees the new table). Run it with the stream stopped, or between catch-up runs.
"""

from __future__ import annotations

import os
import sys

from markets_data import trades
from markets_data.jobs import trades_compare
from markets_data.spark import session
from markets_data.swap import PLANS, renames


def main() -> int:
    target = os.environ.get("TO", "")
    if target not in PLANS:
        print("[swap] usage: TO=v1|v2 (v2 makes the replayed table live; v1 rolls back)")
        return 2
    spark = session("markets-trades-swap")
    steps = renames(target, spark.catalog.tableExists)
    if target == "v2" and os.environ.get("FORCE") != "1" and trades_compare.main() != 0:
        print("[swap] refused: the replay differs from the live table (FORCE=1 to switch anyway)")
        return 1
    for old, new in steps:
        spark.sql(f"ALTER TABLE {old} RENAME TO {new}")
        print(f"[swap] renamed {old} -> {new}", flush=True)
    live = spark.table(trades.SILVER)
    print(
        f"[swap] {trades.SILVER} is now {target}: {live.count()} trades, "
        f"columns {'with' if 'notional' in live.columns else 'without'} notional"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
