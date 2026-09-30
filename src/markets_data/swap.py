"""Which renames exchange the live silver table with the replayed one (no Spark: unit-tested).

Used by markets_data.jobs.trades_swap, which runs them.
"""

from markets_data import trades

# (renames in order, the table that must exist, the table that must not)
PLANS = {
    "v2": ([(trades.SILVER, trades.V1), (trades.V2, trades.SILVER)], trades.V2, trades.V1),
    "v1": ([(trades.SILVER, trades.V2), (trades.V1, trades.SILVER)], trades.V1, trades.V2),
}


def renames(target: str, exists) -> list[tuple[str, str]]:
    """The renames for a switch to `target`, or SystemExit saying why it can't be done."""
    steps, needs, free = PLANS[target]
    if not exists(needs):
        raise SystemExit(f"[swap] {needs} does not exist: nothing to switch to")
    if exists(free):
        raise SystemExit(f"[swap] {free} already exists: switch back or drop it first")
    return steps
