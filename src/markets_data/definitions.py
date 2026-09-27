"""Dagster code location for Markets & Payments Intelligence.

The platform runs it as `tenant-markets-data-code` (generated from its tenants/markets-data.yaml);
`docker compose up` in this repo runs the same image locally against the platform's networks.
Each asset shells out to spark-submit as this tenant's own Polaris principal.
"""

import os
from pathlib import Path

import dagster as dg

JOBS = Path(__file__).with_name("jobs")
OWNERS = ["team:markets-data"]


def spark_submit(script: str) -> list[str]:
    return [
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "--driver-memory",
        os.environ.get("SPARK_DRIVER_MEMORY", "768m"),
        "--conf",
        "spark.ui.showConsoleProgress=false",
        str(JOBS / script),
    ]


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

defs = dg.Definitions(
    assets=[fx_rates],
    schedules=[fx_daily],
    resources={"pipes": dg.PipesSubprocessClient()},
)
