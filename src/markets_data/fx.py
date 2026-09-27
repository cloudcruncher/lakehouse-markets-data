"""ECB reference rates from the Frankfurter API (free, no key): euro against ~30 currencies,
one fixing per business day around 16:00 CET."""

from __future__ import annotations

from datetime import date

import requests

API = "https://api.frankfurter.dev/v1"
SOURCE = "ecb-via-frankfurter"


def fetch(start: date, end: date, timeout: float = 30) -> dict:
    """One call for the whole range; the API returns every business day in it."""
    r = requests.get(f"{API}/{start.isoformat()}..{end.isoformat()}", params={"base": "EUR"}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def to_rows(payload: dict) -> list[tuple[date, str, str, float]]:
    """(rate_date, base, quote, rate) per currency per day, sorted, skipping the base itself."""
    base = payload["base"]
    rows = [
        (date.fromisoformat(day), base, quote, float(rate))
        for day, rates in payload.get("rates", {}).items()
        for quote, rate in rates.items()
        if quote != base
    ]
    return sorted(rows)
