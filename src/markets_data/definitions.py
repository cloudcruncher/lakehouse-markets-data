"""Dagster code location for Markets & Payments Intelligence.

The platform runs it as `tenant-markets-data-code` (generated from its tenants/markets-data.yaml);
`docker compose up` in this repo runs the same image locally against the platform's networks.
Each asset shells out to spark-submit as this tenant's own Polaris principal.
"""

import dagster as dg

from markets_data.submit import spark_submit

OWNERS = ["team:markets-data"]


@dg.asset(
    key=["markets_bronze", "fx_rates"],
    group_name="reference",
    owners=OWNERS,
    kinds={"spark", "iceberg"},
    description="ECB euro reference rates, one row per currency per business day (Frankfurter API).",
)
def fx_rates(context: dg.AssetExecutionContext, pipes: dg.PipesSubprocessClient):
    return pipes.run(command=spark_submit("fx_rates.py"), context=context).get_materialize_result()


fx_daily = dg.ScheduleDefinition(
    name="fx_rates_daily",
    target=[fx_rates],
    # The ECB publishes around 16:00 CET; weekdays only.
    cron_schedule="30 16 * * 1-5",
    execution_timezone="Europe/Berlin",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            ["markets_bronze", "sanctions_targets"],
            group_name="reference",
            owners=OWNERS,
            kinds={"spark", "iceberg"},
            description="UK Sanctions List (FCDO via OpenSanctions): every target ever listed, "
            "with delisted_at once it leaves the list.",
        ),
        dg.AssetSpec(
            ["markets_silver", "sanctions_names"],
            group_name="reference",
            owners=OWNERS,
            kinds={"spark", "iceberg"},
            deps=[["markets_bronze", "sanctions_targets"]],
            description="Normalised names and aliases of listed organisations, for merchant screening.",
        ),
    ],
)
def sanctions(context: dg.AssetExecutionContext, pipes: dg.PipesSubprocessClient):
    return pipes.run(command=spark_submit("sanctions.py"), context=context).get_results()


sanctions_daily = dg.ScheduleDefinition(
    name="sanctions_daily",
    target=[sanctions],
    # OpenSanctions rebuilds the FCDO dataset through the day; screening wants it every morning.
    cron_schedule="15 6 * * *",
    execution_timezone="Europe/London",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

GOLD_SPECS = [
    (
        ["markets_gold", "crypto_ohlcv_1m"],
        "One-minute candles per Coinbase product: open, high, low, close, volume, VWAP.",
        ["candles_are_consistent", "one_candle_per_product_minute"],
    ),
    (
        ["markets_gold", "card_auth_daily"],
        "Card authorisations per day, merchant country, currency and channel, in euro, with approval rate.",
        ["approval_rate_is_a_share", "daily_counts_match_silver"],
    ),
    (
        ["markets_gold", "sanctions_hits"],
        "Merchants whose name matches an organisation on today's UK Sanctions List.",
        ["hits_are_on_todays_list"],
    ),
]


# What each gold table is built from: the asset graph shows it, and the data contract's `upstream`
# says the same (tests/test_definitions.py keeps the two in step). Silver is written by the streams,
# not by Dagster, so those keys appear as external assets.
GOLD_UPSTREAM = {
    "crypto_ohlcv_1m": [["markets_silver", "trades"]],
    "card_auth_daily": [["markets_silver", "card_auths"]],
    "sanctions_hits": [
        ["markets_silver", "card_auths"],
        ["markets_silver", "sanctions_names"],
        ["markets_bronze", "sanctions_targets"],
    ],
}


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            key,
            group_name="gold",
            owners=OWNERS,
            kinds={"spark", "iceberg"},
            description=desc,
            deps=GOLD_UPSTREAM[key[1]],
        )
        for key, desc, _ in GOLD_SPECS
    ],
    check_specs=[dg.AssetCheckSpec(c, asset=key) for key, _, checks in GOLD_SPECS for c in checks],
)
def gold(context: dg.AssetExecutionContext, pipes: dg.PipesSubprocessClient):
    return pipes.run(command=spark_submit("gold.py"), context=context).get_results()


gold_hourly = dg.ScheduleDefinition(
    name="gold_hourly",
    target=[gold],
    cron_schedule="20 * * * *",  # off the hour, when the platform's own jobs run
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


def _never_materialized(instance, asset) -> bool:
    return any(instance.get_latest_materialization_event(spec.key) is None for spec in asset.specs)


@dg.sensor(
    name="first_run",
    description="A new stack has no FX, sanctions or gold until a schedule ticks, and Dagster does not "
    "catch up missed ticks. Runs each once when it has never run; gold waits for both.",
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.RUNNING,
    target=[fx_rates, sanctions, gold],
)
def first_run(context: dg.SensorEvaluationContext):
    reference = [("fx_rates", fx_rates), ("sanctions", sanctions)]
    todo = [name for name, asset in reference if _never_materialized(context.instance, asset)]
    if todo:
        return [
            dg.RunRequest(run_key=f"first-run-{name}", asset_selection=_keys(a))
            for name, a in reference
            if name in todo
        ]
    if _never_materialized(context.instance, gold):
        return [dg.RunRequest(run_key="first-run-gold", asset_selection=_keys(gold))]
    return []


def _keys(asset) -> list[dg.AssetKey]:
    return [spec.key for spec in asset.specs]


defs = dg.Definitions(
    assets=[fx_rates, sanctions, gold],
    schedules=[fx_daily, sanctions_daily, gold_hourly],
    sensors=[first_run],
    resources={"pipes": dg.PipesSubprocessClient()},
)
