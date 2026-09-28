"""Parsing the FCDO list and the name normalisation screening compares on."""

from datetime import datetime

from markets_data import sanctions
from markets_data.submit import JOBS

HEADER = (
    '"id","schema","name","aliases","birth_date","countries","addresses","identifiers","sanctions",'
    '"phones","emails","program_ids","dataset","first_seen","last_seen","last_change"\n'
)


def csv_row(id, schema, name, aliases="", birth="", countries="ru"):
    return (
        f'"{id}","{schema}","{name}","{aliases}","{birth}","{countries}","1 Main St","","UK: Russia regs",'
        f'"+7 000","x@y.ru","GB-RUS","gb_fcdo_sanctions","2022-02-24T00:00:00","2026-09-28T06:00:00",'
        f'"2026-09-15T10:40:01"\n'
    )


SAMPLE = (
    HEADER
    + csv_row("NK-1", "Organization", "PJSC Aeroflot")
    + csv_row("NK-2", "Organization", "AK Alrosa PAO", aliases="ALROSA Company PJSC;Alrosa;Алроса")
    + csv_row("NK-3", "Person", "Ivan Example", birth="1970-01-01")
    + csv_row("NK-4", "Vessel", "SEA STAR")
    + csv_row("NK-5", "Company", "OOO AO", countries="")
)


def test_rows_keep_what_screening_needs_and_nothing_personal():
    rows = sanctions.to_rows(SAMPLE)
    assert len(rows) == 5 and all(len(r) == len(sanctions.COLUMNS) for r in rows)
    person = dict(zip(sanctions.COLUMNS, rows[2], strict=True))
    assert person["name"] == "Ivan Example"
    flat = " ".join(str(v) for v in rows[2])
    for personal in ("1970-01-01", "1 Main St", "+7 000", "x@y.ru"):
        assert personal not in flat
    assert person["last_change"] == datetime(2026, 9, 15, 10, 40, 1)
    assert dict(zip(sanctions.COLUMNS, rows[4], strict=True))["countries"] is None


def test_normalise_drops_case_accents_punctuation_and_legal_forms():
    assert sanctions.normalise('PJSC "Aeroflot"') == "aeroflot"
    assert sanctions.normalise("Aeroflot") == "aeroflot"
    assert sanctions.normalise("Zalando SE") == "zalando"
    assert sanctions.normalise("Société Générale S.A.") == "societe generale"
    assert sanctions.normalise("Booking.com") == "bookingcom"


def test_normalise_refuses_names_too_short_to_screen():
    assert sanctions.normalise("OOO AO") is None
    assert sanctions.normalise("ABC Ltd") is None
    assert sanctions.normalise("") is None and sanctions.normalise(None) is None


def test_screening_names_cover_organisations_and_their_aliases_only():
    names = sanctions.screening_names(sanctions.to_rows(SAMPLE))
    got = {(t, n) for t, _, n, *_ in names}
    assert got == {
        ("NK-1", "aeroflot"),
        ("NK-2", "ak alrosa"),
        ("NK-2", "alrosa"),
        ("NK-2", "алроса"),
    }


def test_the_job_is_shipped():
    assert (JOBS / "sanctions.py").is_file()
