from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelineMetrics:
    input_row_count: int
    valid_row_count: int
    invalid_row_count: int
    duplicate_count: int
    output_row_count: int
    processing_duration_seconds: float
    rows_per_second: float
    number_of_symbols: int
    processed_symbols: list[str]
    input_size_bytes: int
    output_size_bytes: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def save(self, path: str | Path) -> Path:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return output_path
