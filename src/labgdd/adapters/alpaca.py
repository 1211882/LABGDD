"""Bounded Alpaca REST access with an injectable HTTP transport."""

import math
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from http.client import HTTPMessage
from typing import IO

from labgdd.config import ConfigurationError
from labgdd.historical_config import HistoricalRequest


class AcquisitionError(RuntimeError):
    """Controlled error codes only; never include provider bodies or credentials."""


@dataclass(frozen=True)
class Credentials:
    key: str = field(repr=False)
    secret: str = field(repr=False)

    @classmethod
    def from_environment(cls) -> "Credentials":
        key = os.environ.get("ALPACA_API_KEY", "")
        secret = os.environ.get("ALPACA_API_SECRET", "")
        if not key.strip() or not secret.strip() or any(c in key + secret for c in "\r\n"):
            raise ConfigurationError("Alpaca credentials: missing or invalid environment variables")
        return cls(key, secret)


@dataclass(frozen=True)
class HttpResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes


Transport = Callable[[Mapping[str, str], Mapping[str, str], float], HttpResponse]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> None:
        raise AcquisitionError("redirect_rejected")


def http_transport(
    params: Mapping[str, str], headers: Mapping[str, str], timeout: float
) -> HttpResponse:
    url = "https://data.alpaca.markets/v2/stocks/bars?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers=dict(headers))
    try:
        with urllib.request.build_opener(_NoRedirect()).open(request, timeout=timeout) as response:
            body = response.read(16 * 1024 * 1024 + 1)
            if len(body) > 16 * 1024 * 1024:
                raise AcquisitionError("response_too_large")
            return HttpResponse(response.status, dict(response.headers.items()), body)
    except urllib.error.HTTPError as error:
        error.close()
        return HttpResponse(error.code, dict(error.headers.items()), b"")


class AlpacaREST:
    def __init__(
        self,
        credentials: Credentials,
        transport: Transport = http_transport,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.credentials = credentials
        self.transport = transport
        self.sleep = sleep
        self.attempts = 0

    def fetch(self, request: HistoricalRequest, token: str | None) -> HttpResponse:
        params = request.parameters()
        if token is not None:
            params["page_token"] = token
        headers = {
            "APCA-API-KEY-ID": self.credentials.key,
            "APCA-API-SECRET-KEY": self.credentials.secret,
        }
        for attempt in range(request.max_attempts):
            self.attempts += 1
            try:
                response = self.transport(params, headers, request.timeout_seconds)
            except (urllib.error.URLError, TimeoutError, OSError):
                response = HttpResponse(503, {}, b"")
            if response.status == 200:
                return response
            if response.status not in {429, 500, 502, 503, 504}:
                raise AcquisitionError(f"http_{response.status}")
            if attempt + 1 == request.max_attempts:
                raise AcquisitionError("retry_exhausted")
            delay = min(0.25 * 2**attempt, 10.0)
            retry_after = {k.lower(): v for k, v in response.headers.items()}.get("retry-after")
            if retry_after is not None:
                try:
                    recommended = float(retry_after)
                except ValueError:
                    try:
                        recommended = max(
                            0.0,
                            (
                                parsedate_to_datetime(retry_after) - datetime.now(UTC)
                            ).total_seconds(),
                        )
                    except (ValueError, TypeError, OverflowError):
                        raise AcquisitionError("invalid_retry_after") from None
                if not math.isfinite(recommended) or not 0 <= recommended <= 30:
                    raise AcquisitionError("retry_after_exceeds_budget")
                delay = max(delay, recommended)
            self.sleep(delay)
        raise AcquisitionError("retry_exhausted")
