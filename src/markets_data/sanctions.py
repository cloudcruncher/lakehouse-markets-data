"""The UK Sanctions List (FCDO), as OpenSanctions republishes it daily: parsing, and the name
normalisation that merchant screening compares on.

No Spark here, so parsing and normalisation are unit-tested in seconds. The same `normalise()`
runs on both sides of a screen (listed names, merchant names), so they can't drift apart.

Data minimisation: screening needs names, aliases, the kind of entity and its programme, so birth
dates, addresses, phone numbers and emails are never landed.
"""

from __future__ import annotations

import csv
import io
import re
import unicodedata
from datetime import datetime

import requests

DATASET = "gb_fcdo_sanctions"
URL = f"https://data.opensanctions.org/datasets/latest/{DATASET}/targets.simple.csv"
SOURCE = f"opensanctions:{DATASET}"  # CC BY-NC 4.0: fine for this non-commercial showcase

BRONZE = "markets_bronze.sanctions_targets"
SILVER = "markets_silver.sanctions_names"
# Merchants are businesses: people and vessels stay in bronze but aren't screened against.
SCREENED_SCHEMAS = ("Organization", "Company", "LegalEntity")
# Normalised names shorter than this ("bank", "ao") would match far too much.
MIN_NAME_LENGTH = 4

# Legal forms and filler that differ between a register and a card network's merchant name.
LEGAL_FORMS = frozenset(
    "ao cjsc co company corp corporation gmbh inc jsc limited liability llc ltd oao ojsc ooo pao "
    "pjsc plc public sa sarl se zao ag bv nv joint stock closed open the".split()
)

COLUMNS = [
    "target_id", "schema", "name", "aliases", "countries", "program_ids", "sanctions",
    "first_seen", "last_seen", "last_change",
]  # fmt: skip


def fetch(timeout: float = 60) -> str:
    r = requests.get(URL, timeout=timeout)
    r.raise_for_status()
    return r.text


def _time(value: str) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def to_rows(text: str) -> list[tuple]:
    """One tuple per listed target, in COLUMNS order; list-valued fields stay ';'-joined."""
    csv.field_size_limit(1 << 24)  # a target's sanctions text can run to hundreds of KiB
    return [
        (
            r["id"],
            r["schema"],
            r["name"],
            r["aliases"] or None,
            r["countries"] or None,
            r["program_ids"] or None,
            r["sanctions"] or None,
            _time(r["first_seen"]),
            _time(r["last_seen"]),
            _time(r["last_change"]),
        )
        for r in csv.DictReader(io.StringIO(text))
    ]


def normalise(name: str | None) -> str | None:
    """Lower case, no accents or punctuation, no legal forms: 'PJSC "Aeroflot"' -> 'aeroflot'.

    None when nothing distinctive is left (too short to screen on).
    """
    if not name:
        return None
    ascii_ish = "".join(
        c for c in unicodedata.normalize("NFKD", name) if not unicodedata.combining(c)
    ).casefold()
    # Dots join (S.A. -> sa, Booking.com -> bookingcom); anything else that isn't a letter or digit splits.
    words = [w for w in re.split(r"[^\w]+", ascii_ish.replace(".", "")) if w and w not in LEGAL_FORMS]
    out = " ".join(words)
    return out if len(out) >= MIN_NAME_LENGTH else None


def screening_names(rows: list[tuple]) -> list[tuple[str, str, str, str | None, str | None]]:
    """(target_id, schema, name_norm, countries, program_ids): every distinct normalised name and
    alias of every screened target."""
    out = set()
    for target_id, schema, name, aliases, countries, program_ids, *_ in rows:
        if schema not in SCREENED_SCHEMAS:
            continue
        for n in [name, *(aliases or "").split(";")]:
            if norm := normalise(n):
                out.add((target_id, schema, norm, countries, program_ids))
    return sorted(out)
