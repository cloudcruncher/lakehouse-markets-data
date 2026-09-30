import pytest

from markets_data import trades
from markets_data.swap import renames


def tables(*names):
    return lambda name: name in names


def test_forward_swap_moves_the_live_table_aside_then_promotes_the_replay():
    assert renames("v2", tables(trades.SILVER, trades.V2)) == [
        (trades.SILVER, trades.V1),
        (trades.V2, trades.SILVER),
    ]


def test_rollback_is_the_same_exchange_the_other_way():
    assert renames("v1", tables(trades.SILVER, trades.V1)) == [
        (trades.SILVER, trades.V2),
        (trades.V1, trades.SILVER),
    ]


def test_nothing_to_switch_to_is_refused():
    with pytest.raises(SystemExit, match="does not exist"):
        renames("v2", tables(trades.SILVER))


def test_a_second_swap_cannot_overwrite_the_table_kept_for_rollback():
    with pytest.raises(SystemExit, match="already exists"):
        renames("v2", tables(trades.SILVER, trades.V2, trades.V1))
