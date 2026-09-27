"""The code location loads, and says what the platform will run."""

import dagster as dg

from markets_data.definitions import defs
from markets_data.submit import spark_submit


def test_code_location_loads():
    graph = defs.resolve_asset_graph()
    assert graph.get_all_asset_keys() == {dg.AssetKey(["markets_bronze", "fx_rates"])}
    assert [s.name for s in defs.schedules] == ["fx_rates_daily"]


def test_jobs_are_shipped_with_the_package():
    script = spark_submit("fx_rates.py")[-1]
    assert script.endswith("markets_data/jobs/fx_rates.py")
    from pathlib import Path

    assert Path(script).exists()
