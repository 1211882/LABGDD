from __future__ import annotations

import json
import platform
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class StreamingIngestionMetrics:
    endpoint: str
    feed: str
    channel: str
    symbols: list[str]
    test_stream: bool
    started_at_utc: str = field(default_factory=_utc_now)
    finished_at_utc: str | None = None
    duration_seconds: float = 0.0
    events_received: int = 0
    events_published: int = 0
    ignored_messages: int = 0
    malformed_messages: int = 0
    connection_count: int = 0
    reconnect_count: int = 0
    delivery_failures: int = 0
    events_per_second: float = 0.0
    status: str = "running"
    error: str | None = None
    _started_monotonic: float = field(default_factory=time.perf_counter, repr=False)

    def finish(self, status: str, error: str | None = None) -> None:
        self.finished_at_utc = _utc_now()
        self.duration_seconds = round(time.perf_counter() - self._started_monotonic, 6)
        self.events_per_second = round(
            self.events_published / self.duration_seconds
            if self.duration_seconds > 0
            else 0.0,
            3,
        )
        self.status = status
        self.error = error

    def to_dict(self) -> dict[str, object]:
        return {
            "benchmark": "Alpaca WebSocket to Kafka ingestion",
            "environment": {
                "os": platform.platform(),
                "python": platform.python_version(),
                "websockets": version("websockets"),
                "confluent_kafka": version("confluent-kafka"),
            },
            "endpoint": self.endpoint,
            "feed": self.feed,
            "channel": self.channel,
            "symbols": self.symbols,
            "number_of_symbols": len(self.symbols),
            "test_stream": self.test_stream,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": self.finished_at_utc,
            "duration_seconds": self.duration_seconds,
            "events_received": self.events_received,
            "events_published": self.events_published,
            "events_per_second": self.events_per_second,
            "ignored_messages": self.ignored_messages,
            "malformed_messages": self.malformed_messages,
            "connection_count": self.connection_count,
            "reconnect_count": self.reconnect_count,
            "delivery_failures": self.delivery_failures,
            "status": self.status,
            "error": self.error,
        }

    def save(self, path: str | Path) -> Path:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return output_path
