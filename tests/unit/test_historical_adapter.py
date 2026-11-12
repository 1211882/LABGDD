import io
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from http.client import HTTPMessage
from unittest.mock import MagicMock

import pytest

from labgdd.adapters.alpaca import (
    AcquisitionError,
    AlpacaREST,
    Credentials,
    HttpResponse,
    _NoRedirect,
    http_transport,
)
from labgdd.config import ConfigurationError
from labgdd.historical_config import HistoricalRequest


def request() -> HistoricalRequest:
    return HistoricalRequest(
        "fixture-v1",
        ("AAPL",),
        datetime(2026, 11, 2, 14, 30, tzinfo=UTC),
        datetime(2026, 11, 2, 14, 36, tzinfo=UTC),
        date(2026, 11, 3),
    )


def test_authentication_environment_is_required_and_not_represented(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET", raising=False)
    with pytest.raises(ConfigurationError):
        Credentials.from_environment()
    monkeypatch.setenv("ALPACA_API_KEY", "synthetic-key")
    monkeypatch.setenv("ALPACA_API_SECRET", "synthetic-secret")
    credentials = Credentials.from_environment()
    assert "synthetic" not in repr(credentials)
    monkeypatch.setenv("ALPACA_API_SECRET", "invalid\nheader")
    with pytest.raises(ConfigurationError):
        Credentials.from_environment()


def test_explicit_request_parameters_auth_timeout_and_token() -> None:
    def transport(
        params: Mapping[str, str], headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        assert params["feed"] == "iex" and params["adjustment"] == "raw"
        assert params["timeframe"] == "1Min" and params["page_token"] == "continuation"
        assert params["end"] == "2026-11-02T14:35:59.999999+00:00"
        assert headers["APCA-API-KEY-ID"] == "synthetic-key"
        assert timeout == 15
        return HttpResponse(200, {}, b"{}")

    assert (
        AlpacaREST(Credentials("synthetic-key", "synthetic-secret"), transport)
        .fetch(request(), "continuation")
        .status
        == 200
    )


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_transient_failures_retry_then_succeed(status: int) -> None:
    calls = 0
    sleeps: list[float] = []

    def transport(
        params: Mapping[str, str], headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        nonlocal calls
        calls += 1
        return HttpResponse(status if calls == 1 else 200, {"Retry-After": "2"}, b"{}")

    client = AlpacaREST(Credentials("fixture", "fixture"), transport, sleeps.append)
    assert client.fetch(request(), None).status == 200
    assert calls == 2 and sleeps == [2.0]


def test_retry_exhaustion_and_timeout_are_bounded() -> None:
    sleeps: list[float] = []

    def transport(
        params: Mapping[str, str], headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        raise TimeoutError("do not expose underlying error")

    client = AlpacaREST(Credentials("fixture", "fixture"), transport, sleeps.append)
    with pytest.raises(AcquisitionError, match="^retry_exhausted$"):
        client.fetch(request(), None)
    assert client.attempts == 3 and sleeps == [0.25, 0.5]


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_terminal_errors_do_not_retry_or_echo_body(status: int) -> None:
    client = AlpacaREST(
        Credentials("fixture", "fixture"),
        lambda p, h, t: HttpResponse(status, {}, b"sensitive-response"),
    )
    with pytest.raises(AcquisitionError) as failure:
        client.fetch(request(), None)
    assert str(failure.value) == f"http_{status}" and client.attempts == 1


@pytest.mark.parametrize("header", ["1000", "Infinity", "NaN", "-1", "not-a-date"])
def test_unusable_retry_after_fails_without_early_retry(header: str) -> None:
    client = AlpacaREST(
        Credentials("fixture", "fixture"),
        lambda p, h, t: HttpResponse(429, {"Retry-After": header}, b""),
    )
    with pytest.raises(AcquisitionError):
        client.fetch(request(), None)
    assert client.attempts == 1


def test_retry_after_http_date() -> None:
    from email.utils import format_datetime

    replies = iter(
        [
            HttpResponse(
                429, {"Retry-After": format_datetime(datetime.now(UTC) + timedelta(seconds=5))}, b""
            ),
            HttpResponse(200, {}, b"{}"),
        ]
    )
    sleeps: list[float] = []
    client = AlpacaREST(
        Credentials("fixture", "fixture"), lambda p, h, t: next(replies), sleeps.append
    )
    client.fetch(request(), None)
    assert len(sleeps) == 1 and 0.25 <= sleeps[0] <= 5


def test_http_transport_opener_is_mocked(monkeypatch: pytest.MonkeyPatch) -> None:
    response = MagicMock()
    response.status = 200
    response.headers.items.return_value = []
    response.read.return_value = b'{"bars":{}}'
    opener = MagicMock()
    opener.open.return_value.__enter__.return_value = response
    monkeypatch.setattr("urllib.request.build_opener", lambda *args: opener)
    assert http_transport(request().parameters(), {}, 15).body == b'{"bars":{}}'
    assert opener.open.call_args.kwargs["timeout"] == 15
    req = opener.open.call_args.args[0]
    assert req.full_url.startswith("https://data.alpaca.markets/v2/stocks/bars?")


def test_redirects_are_rejected() -> None:
    import urllib.request

    with pytest.raises(AcquisitionError, match="redirect_rejected"):
        _NoRedirect().redirect_request(
            urllib.request.Request("https://data.alpaca.markets"),
            io.BytesIO(),
            302,
            "redirect",
            HTTPMessage(),
            "https://example.invalid",
        )
