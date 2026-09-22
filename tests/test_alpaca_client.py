from __future__ import annotations

import pytest

from src.ingestion.alpaca_client import AlpacaApiError, AlpacaClient


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.ok = 200 <= status_code < 300

    def json(self) -> dict:
        if self._payload is None:
            raise ValueError("invalid json")
        return self._payload


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.headers: dict[str, str] = {}
        self.responses = responses
        self.calls: list[dict] = []

    def get(self, url: str, params: dict, timeout: int) -> FakeResponse:
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return self.responses.pop(0)


def make_client(session: FakeSession) -> AlpacaClient:
    return AlpacaClient(
        base_url="https://data.alpaca.markets/v2",
        api_key_id="key",
        api_secret_key="secret",
        session=session,
    )


def test_get_historical_bars_successful_response() -> None:
    session = FakeSession(
        [
            FakeResponse(
                200,
                {
                    "bars": [
                        {"t": "2026-09-01T09:30:00Z", "o": 1, "h": 2, "l": 1, "c": 2, "v": 10}
                    ]
                },
            )
        ]
    )

    bars = make_client(session).get_historical_bars(
        symbol="AAPL",
        start="2026-09-01",
        end="2026-09-20",
        timeframe="1Min",
        limit=1000,
        feed="iex",
    )

    assert len(bars) == 1
    assert session.calls[0]["url"].endswith("/stocks/AAPL/bars")
    assert session.calls[0]["params"]["feed"] == "iex"


def test_get_historical_bars_authentication_error() -> None:
    session = FakeSession([FakeResponse(401, {"message": "authentication failed"})])

    with pytest.raises(AlpacaApiError, match="401"):
        make_client(session).get_historical_bars(
            symbol="AAPL",
            start="2026-09-01",
            end="2026-09-20",
            timeframe="1Min",
            limit=1000,
            feed="iex",
        )


def test_get_historical_bars_empty_response() -> None:
    session = FakeSession([FakeResponse(200, {"bars": []})])

    bars = make_client(session).get_historical_bars(
        symbol="AAPL",
        start="2026-09-01",
        end="2026-09-20",
        timeframe="1Min",
        limit=1000,
        feed="iex",
    )

    assert bars == []


def test_get_historical_bars_pagination() -> None:
    session = FakeSession(
        [
            FakeResponse(
                200,
                {
                    "bars": [
                        {"t": "2026-09-01T09:30:00Z", "o": 1, "h": 2, "l": 1, "c": 2, "v": 10}
                    ],
                    "next_page_token": "next-token",
                },
            ),
            FakeResponse(
                200,
                {
                    "bars": [
                        {"t": "2026-09-01T09:31:00Z", "o": 2, "h": 3, "l": 2, "c": 3, "v": 20}
                    ]
                },
            ),
        ]
    )

    bars = make_client(session).get_historical_bars(
        symbol="AAPL",
        start="2026-09-01",
        end="2026-09-20",
        timeframe="1Min",
        limit=1,
        feed="iex",
    )

    assert len(bars) == 2
    assert len(session.calls) == 2
    assert session.calls[1]["params"]["page_token"] == "next-token"
