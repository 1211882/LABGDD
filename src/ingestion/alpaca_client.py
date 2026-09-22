from __future__ import annotations

import logging
from typing import Any

import requests

LOGGER = logging.getLogger(__name__)


class AlpacaApiError(RuntimeError):
    """Raised when Alpaca returns an unsuccessful response."""


class AlpacaClient:
    """Small HTTP client wrapper for Alpaca Market Data API v2."""

    def __init__(
        self,
        base_url: str,
        api_key_id: str,
        api_secret_key: str,
        timeout_seconds: int = 30,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "APCA-API-KEY-ID": api_key_id,
                "APCA-API-SECRET-KEY": api_secret_key,
            }
        )

    def get_historical_bars(
        self,
        symbol: str,
        start: str,
        end: str,
        timeframe: str,
        limit: int,
        feed: str,
    ) -> list[dict[str, Any]]:
        """Fetch all historical bar pages for one stock symbol."""
        all_bars: list[dict[str, Any]] = []
        next_page_token: str | None = None

        while True:
            payload = self._get(
                f"/stocks/{symbol}/bars",
                params={
                    "start": start,
                    "end": end,
                    "timeframe": timeframe,
                    "limit": limit,
                    "feed": feed,
                    **({"page_token": next_page_token} if next_page_token else {}),
                },
            )
            bars = payload.get("bars", [])
            if not isinstance(bars, list):
                raise AlpacaApiError(f"Unexpected bars payload for {symbol}: {type(bars)}")

            all_bars.extend(bars)
            next_page_token = payload.get("next_page_token")
            if not next_page_token:
                break

        return all_bars

    def get_multiple_stock_bars(
        self,
        symbols: list[str],
        start: str,
        end: str,
        timeframe: str,
        limit: int,
        feed: str,
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch historical bars for multiple symbols."""
        return {
            symbol: self.get_historical_bars(
                symbol=symbol,
                start=start,
                end=end,
                timeframe=timeframe,
                limit=limit,
                feed=feed,
            )
            for symbol in symbols
        }

    def health_check(self) -> bool:
        """Perform a lightweight authenticated request against market data."""
        try:
            self._get(
                "/stocks/AAPL/bars",
                params={"timeframe": "1Min", "limit": 1, "feed": "iex"},
            )
        except AlpacaApiError:
            LOGGER.exception("Alpaca health check failed")
            return False
        return True

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        try:
            response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        except requests.RequestException as exc:
            raise AlpacaApiError(f"Request to Alpaca failed: {exc}") from exc

        if not response.ok:
            message = _response_message(response)
            raise AlpacaApiError(
                f"Alpaca API error {response.status_code} for {url}: {message}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise AlpacaApiError("Alpaca returned invalid JSON.") from exc

        if not isinstance(payload, dict):
            raise AlpacaApiError(f"Unexpected Alpaca payload type: {type(payload)}")
        return payload


def _response_message(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text
    if isinstance(payload, dict):
        return str(payload.get("message") or payload.get("error") or payload)
    return str(payload)
