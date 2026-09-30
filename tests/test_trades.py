from markets_data import trades
from markets_data.submit import JOBS


def test_every_rule_has_a_distinct_reason():
    reasons = [r for r, _ in trades.REJECT_RULES]
    assert len(reasons) == len(set(reasons))


def test_reason_is_the_first_rule_that_matches():
    sql = trades.reject_reason_sql()
    assert sql.startswith("CASE WHEN trade_id IS NULL THEN 'not a trade record' WHEN ")
    assert sql.endswith("WHEN trade_time IS NULL THEN 'time is not a timestamp' END")


def test_silver_ddl_has_the_silver_columns_in_order():
    body = trades.SILVER_DDL.split("(", 1)[1].split(") USING", 1)[0]
    assert [c.split()[0] for c in body.replace("\n", " ").split(", ")] == trades.SILVER_COLUMNS


def test_the_stream_job_is_shipped():
    assert (JOBS / "trades_stream.py").is_file()


def test_v2_ddl_is_silver_plus_notional_in_order():
    body = trades.V2_DDL.split("(", 1)[1].split(") USING", 1)[0]
    assert [c.split()[0] for c in body.replace("\n", " ").split(", ")] == trades.V2_COLUMNS
    assert trades.V2_COLUMNS == [*trades.SILVER_COLUMNS, "notional"]


def test_the_replay_jobs_are_shipped():
    assert (JOBS / "trades_replay.py").is_file()
    assert (JOBS / "trades_compare.py").is_file()
