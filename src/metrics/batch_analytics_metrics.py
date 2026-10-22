from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class BatchAnalyticsMetrics:
    dataset_version: str
    generated_at_utc: str
    regular_session_only: bool
    input_rows: int
    rows_after_session_filter: int
    number_of_symbols: int
    number_of_sectors: int
    symbol_windows: dict[str, int]
    sector_windows: dict[str, int]
    symbol_windows_with_missing_minutes: dict[str, int]
    coverage_by_duration: dict[str, dict[str, dict[str, float | int]]]
    processing_duration_seconds: float
    rows_per_second: float
    output_size_bytes: int
    output_root: str

    @classmethod
    def timestamp(cls) -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def save(self, path: str | Path) -> Path:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True), encoding="utf-8"
        )
        return output
