"""spark-submit a job shipped with this package, as this tenant.

Dagster assets call `spark_submit()`; the platform runs the streams as a tenant service with
`python3 -m markets_data.submit streams.py`, since the package's install path differs by image
and Python version. At laptop scale (markets_data.scale) that service is a loop: one catch-up
run every RUN_EVERY seconds, with no JVM held in between.
"""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from markets_data.scale import RUN_EVERY, TRIGGER

JOBS = Path(__file__).with_name("jobs")
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
# A streams driver (for weeks at full scale, minutes a run on a laptop): the JVM needs ~450 MB beside its heap
# (metaspace ~210 MB, ~300 threads, code cache), and a stream's live heap is ~250 MB after GC
# (measured 28 Sep 2026). Batch jobs run in the code server and keep the larger default.
# Both streams in one application (streams.py, the platform's service) hold two live heaps.
DRIVER_MEMORY = {"trades_stream.py": "512m", "card_auths_stream.py": "512m", "streams.py": "768m"}


def spark_submit(script: str) -> list[str]:
    return [
        SPARK_SUBMIT,
        "--master",
        "local[2]",
        "--driver-memory",
        os.environ.get("SPARK_DRIVER_MEMORY", DRIVER_MEMORY.get(script, "768m")),
        "--conf",
        "spark.ui.showConsoleProgress=false",
        str(JOBS / script),
    ]


def main() -> None:
    if len(sys.argv) != 2 or not (JOBS / sys.argv[1]).is_file():
        jobs = sorted(p.name for p in JOBS.glob("*.py") if p.name != "__init__.py")
        raise SystemExit(f"usage: python3 -m markets_data.submit JOB  (one of {', '.join(jobs)})")
    cmd = spark_submit(sys.argv[1])
    # glibc gives each busy thread its own malloc arena; with a JVM's ~300 threads that is
    # hundreds of MB of mostly empty memory. Two arenas are plenty.
    os.environ.setdefault("MALLOC_ARENA_MAX", "2")
    if not RUN_EVERY:
        os.execv(cmd[0], cmd)  # spark-submit becomes PID 1's child, and gets SIGTERM directly
    every(cmd, RUN_EVERY)


def every(cmd: list[str], seconds: int) -> None:
    """Run cmd, then again `seconds` after it started, until stopped; a failed run ends the loop
    (non-zero exit), so the platform's restart policy and alerts see it."""
    run: subprocess.Popen | None = None

    def stop(signum, _frame):
        if run and run.poll() is None:
            run.send_signal(signum)  # a catch-up interrupted mid-batch resumes from its checkpoint
            run.wait()
        sys.exit(0)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while True:
        started = time.monotonic()
        print(f"[submit] {Path(cmd[-1]).name}: catch-up run ({TRIGGER})", flush=True)
        run = subprocess.Popen(cmd)
        if code := run.wait():
            raise SystemExit(code)
        time.sleep(max(0.0, seconds - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
