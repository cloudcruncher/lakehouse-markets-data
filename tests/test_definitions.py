"""The code location loads, and says what the platform will run."""

from pathlib import Path

import dagster as dg

from markets_data.definitions import defs
from markets_data.submit import spark_submit

ROOT = Path(__file__).resolve().parents[1]


def test_code_location_loads():
    graph = defs.resolve_asset_graph()
    # Silver trades and card auths are written by the streams, not Dagster: they appear as the
    # external assets gold depends on, so the asset graph shows what gold is built from.
    assert graph.external_asset_keys == {
        dg.AssetKey(["markets_silver", "trades"]),
        dg.AssetKey(["markets_silver", "card_auths"]),
    }
    assert graph.materializable_asset_keys == {
        dg.AssetKey(["markets_bronze", "fx_rates"]),
        dg.AssetKey(["markets_bronze", "sanctions_targets"]),
        dg.AssetKey(["markets_silver", "sanctions_names"]),
        dg.AssetKey(["markets_gold", "crypto_ohlcv_1m"]),
        dg.AssetKey(["markets_gold", "card_auth_daily"]),
        dg.AssetKey(["markets_gold", "sanctions_hits"]),
    }
    assert [s.name for s in defs.schedules] == ["fx_rates_daily", "sanctions_daily", "gold_hourly"]


def test_every_gold_check_the_job_runs_is_declared():
    from markets_data import gold

    declared = {(k.asset_key.to_user_string(), k.name) for k in defs.resolve_asset_graph().asset_check_keys}
    ran = {(table.replace(".", "/"), name) for name, table, _ in gold.CHECKS}
    assert ran == declared


def test_jobs_are_shipped_with_the_package():
    script = spark_submit("fx_rates.py")[-1]
    assert script.endswith("markets_data/jobs/fx_rates.py")
    from pathlib import Path

    assert Path(script).exists()


def test_driver_heap_per_job():
    def heap(script):
        cmd = spark_submit(script)
        return cmd[cmd.index("--driver-memory") + 1]

    assert heap("trades_stream.py") == heap("card_auths_stream.py") == "512m"
    assert heap("fx_rates.py") == heap("sanctions.py") == heap("gold.py") == "768m"
    assert heap("streams.py") == "768m"  # both streams in one application


def test_gold_contract_upstream_matches_the_asset_graph():
    """The catalog shows the contract's `upstream`, Dagster shows the asset deps: they must agree."""
    import yaml

    from markets_data.definitions import GOLD_UPSTREAM

    contract = yaml.safe_load((ROOT / "contracts" / "gold.odcs.yaml").read_text())
    declared = {}
    for obj in contract["schema"]:
        up = next(c["value"] for c in obj["customProperties"] if c["property"] == "upstream")
        declared[obj["name"]] = {u.strip() for u in up.split(",")}
    assert declared == {t: {".".join(k) for k in keys} for t, keys in GOLD_UPSTREAM.items()}
