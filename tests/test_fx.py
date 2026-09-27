from datetime import date

from markets_data import fx

PAYLOAD = {
    "amount": 1.0,
    "base": "EUR",
    "start_date": "2026-09-24",
    "end_date": "2026-09-25",
    "rates": {
        "2026-09-25": {"GBP": 0.86045, "USD": 1.1403},
        "2026-09-24": {"GBP": 0.85986, "USD": 1.1367},
    },
}


def test_one_row_per_currency_per_day_sorted():
    assert fx.to_rows(PAYLOAD) == [
        (date(2026, 9, 24), "EUR", "GBP", 0.85986),
        (date(2026, 9, 24), "EUR", "USD", 1.1367),
        (date(2026, 9, 25), "EUR", "GBP", 0.86045),
        (date(2026, 9, 25), "EUR", "USD", 1.1403),
    ]


def test_empty_range_gives_no_rows():
    assert fx.to_rows({"base": "EUR", "rates": {}}) == []


def test_base_currency_is_never_a_quote():
    assert fx.to_rows({"base": "EUR", "rates": {"2026-09-25": {"EUR": 1.0, "USD": 1.14}}}) == [
        (date(2026, 9, 25), "EUR", "USD", 1.14)
    ]


def test_fetch_asks_for_the_range_against_eur(monkeypatch):
    seen = {}

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return PAYLOAD

    def get(url, params, timeout):
        seen.update(url=url, params=params)
        return Response()

    monkeypatch.setattr(fx.requests, "get", get)
    assert fx.fetch(date(2026, 9, 24), date(2026, 9, 25)) == PAYLOAD
    assert seen == {"url": f"{fx.API}/2026-09-24..2026-09-25", "params": {"base": "EUR"}}
