"""Independently validate pending expected-series Gold and legacy AAPL parity."""

import argparse
import json
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from labgdd.adapters.bronze import sha256


def _manifest(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("status") != "complete":
        raise ValueError("expected Gold evidence: incomplete manifest")
    return cast(dict[str, Any], value)


def _artifacts(path: Path, manifest: dict[str, Any]) -> int:
    entries = manifest.get("artifacts")
    if not isinstance(entries, list) or not entries:
        raise ValueError("expected Gold evidence: invalid inventory")
    expected = {"manifest.json"}
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"path", "sha256", "bytes"}:
            raise ValueError("expected Gold evidence: invalid artifact")
        relative = str(entry["path"])
        item = path.parent / relative
        if item.stat().st_size != entry["bytes"] or sha256(item) != entry["sha256"]:
            raise ValueError("expected Gold evidence: artifact mismatch")
        expected.add(Path(relative).as_posix())
    actual = {
        item.relative_to(path.parent).as_posix()
        for item in path.parent.rglob("*")
        if item.is_file()
    }
    if actual != expected:
        raise ValueError("expected Gold evidence: inventory mismatch")
    return len(entries)


def _rows(manifest: Path, name: str) -> list[dict[str, Any]]:
    return cast(
        list[dict[str, Any]],
        ds.dataset(manifest.parent / name, format="parquet", partitioning="hive")
        .to_table()
        .to_pylist(),
    )


def _physical(manifest: Path) -> dict[str, str]:
    required_timestamps = {
        "returns": {"event_time", "bar_end", "return_end", "session_open", "session_close"},
        "rolling": {"window_start", "window_end", "session_open", "session_close"},
        "fixed": {"window_start", "window_end", "session_open", "session_close"},
    }
    summaries = {}
    for name, timestamps in required_timestamps.items():
        files = sorted((manifest.parent / name).rglob("*.parquet"))
        schemas = [pq.read_schema(item) for item in files]
        if not schemas or any(schema != schemas[0] for schema in schemas[1:]):
            raise ValueError("expected Gold evidence: inconsistent physical schema")
        for field in timestamps | {"membership_valid_from", "membership_valid_to"}:
            if schemas[0].field(field).type != pa.timestamp("us", tz="UTC"):
                raise ValueError("expected Gold evidence: timestamp mismatch")
        membership_timestamp = pa.list_(
            pa.field("element", pa.timestamp("us", tz="UTC"), nullable=False)
        )
        if schemas[0].field("membership_change_times").type != membership_timestamp:
            raise ValueError("expected Gold evidence: membership timestamp mismatch")
        summaries[name] = str(schemas[0])
    return summaries


def _key(row: dict[str, Any], name: str) -> tuple[object, ...]:
    if name == "returns":
        return row["symbol"], row["session_id"], row["event_time"], row["return_end"]
    return row["symbol"], row["session_id"], row["horizon"], row["window_end"]


def _equal_number(left: object, right: object) -> bool:
    if left is None or right is None:
        return left is right
    left_number = float(cast(float, left))
    right_number = float(cast(float, right))
    return abs(left_number - right_number) <= 1e-12 + 1e-10 * abs(right_number)


def _aapl_parity(expected: Path, legacy: Path) -> dict[str, int]:
    values = {
        "returns": ("intrabar_log_return", "close_to_close_log_return"),
        "rolling": ("rolling_return_volatility",),
        "fixed": ("realized_variance", "realized_volatility"),
    }
    counts = {}
    for name, financial in values.items():
        current = {_key(row, name): row for row in _rows(expected, name) if row["symbol"] == "AAPL"}
        previous = {_key(row, name): row for row in _rows(legacy, name) if row["symbol"] == "AAPL"}
        if current.keys() != previous.keys():
            raise ValueError("expected Gold evidence: AAPL key mismatch")
        for key, row in current.items():
            old = previous[key]
            if row["eligible"] != old["eligible"] or row["reason"] != old["reason"]:
                raise ValueError("expected Gold evidence: AAPL eligibility mismatch")
            if any(not _equal_number(row[field], old[field]) for field in financial):
                raise ValueError("expected Gold evidence: AAPL numeric mismatch")
        counts[f"aapl_{name}"] = len(current)
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("expected_manifest", type=Path)
    parser.add_argument("legacy_manifest", type=Path)
    args = parser.parse_args()
    expected = _manifest(args.expected_manifest)
    legacy = _manifest(args.legacy_manifest)
    if expected.get("publication_status") != "pending_architecture_review":
        raise ValueError("expected Gold evidence: publication is not pending review")
    returns = _rows(args.expected_manifest, "returns")
    rolling = _rows(args.expected_manifest, "rolling")
    fixed = _rows(args.expected_manifest, "fixed")
    if (
        len(returns) != expected.get("return_records")
        or len(rolling) != expected.get("rolling_records")
        or len(fixed) != expected.get("fixed_records")
    ):
        raise ValueError("expected Gold evidence: manifest count mismatch")
    absent_returns = [row for row in returns if row["close"] is None]
    if any(
        row["intrabar_log_return"] is not None or row["close_to_close_log_return"] is not None
        for row in absent_returns
    ):
        raise ValueError("expected Gold evidence: absent symbol has a financial value")
    if any(
        row["rolling_return_volatility"] is not None
        for row in rolling
        if row["observed_returns"] == 0
    ) or any(
        row["realized_variance"] is not None or row["realized_volatility"] is not None
        for row in fixed
        if row["observed_returns"] == 0
    ):
        raise ValueError("expected Gold evidence: absent window has a financial value")
    summary: dict[str, object] = {
        "manifest_sha256": sha256(args.expected_manifest),
        "artifacts_verified": _artifacts(args.expected_manifest, expected),
        "physical_schemas": _physical(args.expected_manifest),
        "return_records": len(returns),
        "rolling_records": len(rolling),
        "fixed_records": len(fixed),
        "observed_bar_slots": sum(row["close"] is not None for row in returns),
        "missing_bar_slots": len(absent_returns),
        "eligible_returns": sum(bool(row["eligible"]) for row in returns),
        "eligible_rolling": sum(bool(row["eligible"]) for row in rolling),
        "eligible_fixed": sum(bool(row["eligible"]) for row in fixed),
        **_aapl_parity(args.expected_manifest, args.legacy_manifest),
        "legacy_manifest_sha256": sha256(args.legacy_manifest),
        "legacy_status": legacy["status"],
    }
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
