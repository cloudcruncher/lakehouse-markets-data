"""How this tenant's streams run at the size of platform they are deployed on.

The platform tells every tenant workload its size in PLATFORM_SCALE (open-lakehouse docs/scale.md):

  * laptop (the default): a stream is a scheduled catch-up. Every RUN_EVERY seconds the service
    starts Spark, reads what arrived since its checkpoint (trigger availableNow, in batches of
    at most MAX_OFFSETS_PER_TRIGGER records), commits, and exits. Between runs no JVM is held,
    so silver is up to ~5 minutes behind the topic.
  * full: the same queries run continuously, committing every TRIGGER.

Either way the checkpoints, exactly-once MERGEs and Kappa replay are the same code; only when
Spark runs changes. TRIGGER and RUN_EVERY override the scale's defaults.
"""

from __future__ import annotations

import os

AVAILABLE_NOW = "available-now"
DEFAULTS = {
    "laptop": {"trigger": AVAILABLE_NOW, "run_every": 300},
    "full": {"trigger": "30 seconds", "run_every": 0},
}


def settings(env: dict[str, str] | None = None) -> tuple[str, int]:
    """(trigger, run_every seconds) for this deployment; run_every 0 means run once, for good."""
    env = os.environ if env is None else env
    scale = env.get("PLATFORM_SCALE", "laptop")
    if scale not in DEFAULTS:
        raise SystemExit(f"PLATFORM_SCALE={scale!r}: expected one of {', '.join(DEFAULTS)}")
    trigger = env.get("TRIGGER", DEFAULTS[scale]["trigger"])
    default_every = DEFAULTS[scale]["run_every"] if trigger == AVAILABLE_NOW else 0
    return trigger, int(env.get("RUN_EVERY", default_every))


TRIGGER, RUN_EVERY = settings()


def trigger_kwargs(trigger: str = TRIGGER) -> dict:
    """What DataStreamWriter.trigger() takes for this trigger."""
    return {"availableNow": True} if trigger == AVAILABLE_NOW else {"processingTime": trigger}
