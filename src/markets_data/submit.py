"""spark-submit a job shipped with this package, as this tenant.

Dagster assets call `spark_submit()`; the platform runs long-lived jobs as tenant services with
`python3 -m markets_data.submit trades_stream.py`, since the package's install path differs by
image and Python version.
"""

import os
import sys
from pathlib import Path

JOBS = Path(__file__).with_name("jobs")
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"


def spark_submit(script: str) -> list[str]:
    return [
        SPARK_SUBMIT,
        "--master",
        "local[2]",
        "--driver-memory",
        os.environ.get("SPARK_DRIVER_MEMORY", "768m"),
        "--conf",
        "spark.ui.showConsoleProgress=false",
        str(JOBS / script),
    ]


def main() -> None:
    if len(sys.argv) != 2 or not (JOBS / sys.argv[1]).is_file():
        jobs = sorted(p.name for p in JOBS.glob("*.py") if p.name != "__init__.py")
        raise SystemExit(f"usage: python3 -m markets_data.submit JOB  (one of {', '.join(jobs)})")
    cmd = spark_submit(sys.argv[1])
    os.execv(cmd[0], cmd)  # spark-submit becomes PID 1's child, and gets SIGTERM directly


if __name__ == "__main__":
    main()
