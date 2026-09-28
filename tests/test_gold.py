"""Gold layouts and checks, without Spark (tests/spark/check_gold.py runs the SQL)."""

from markets_data import gold


def columns(ddl):
    body = ddl.split("(", 1)[1].rsplit(") USING", 1)[0]
    return [c.split()[0] for c in body.replace("\n", " ").split(", ")]


def test_gold_holds_no_card_token_or_person():
    for ddl in (gold.OHLCV_DDL, gold.CARD_DAILY_DDL, gold.HITS_DDL):
        assert not {"card_token", "kafka_key", "payload", "name"} & set(columns(ddl))


def test_every_check_counts_offending_rows_in_a_gold_table():
    for name, table, sql in gold.CHECKS:
        assert table in (gold.OHLCV, gold.CARD_DAILY, gold.HITS), name
        assert sql.startswith("SELECT"), name
    assert len({n for n, *_ in gold.CHECKS}) == len(gold.CHECKS)


def test_rebuild_covers_late_records():
    assert gold.REBUILD_DAYS >= 2
