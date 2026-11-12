"""Independently verify a completed Gold run and scalar-reference parity."""

import argparse
import json
from datetime import UTC
from pathlib import Path
from typing import Any, cast

import pyarrow.dataset as ds

from labgdd.adapters.bronze import sha256
from labgdd.analytics.individual import ReferenceBar, calculate_individual_measurements


def _manifest(path: Path) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or document.get("status") != "complete":
        raise ValueError("evidence validation: incomplete manifest")
    return cast(dict[str, Any], document)


def _verify_artifacts(path: Path, document: dict[str, Any]) -> int:
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, list):
        raise ValueError("evidence validation: invalid artifact inventory")
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise ValueError("evidence validation: invalid artifact")
        item = path.parent / str(artifact["path"])
        if item.stat().st_size != artifact["bytes"] or sha256(item) != artifact["sha256"]:
            raise ValueError("evidence validation: artifact mismatch")
    return len(artifacts)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("silver_manifest", type=Path)
    parser.add_argument("gold_manifest", type=Path)
    args = parser.parse_args()
    silver = _manifest(args.silver_manifest)
    gold = _manifest(args.gold_manifest)
    silver_artifacts = _verify_artifacts(args.silver_manifest, silver)
    gold_artifacts = _verify_artifacts(args.gold_manifest, gold)
    if gold.get("silver_manifest_sha256") != sha256(args.silver_manifest):
        raise ValueError("evidence validation: Silver lineage mismatch")
    canonical = ds.dataset(
        args.silver_manifest.parent / "canonical", format="parquet", partitioning="hive"
    ).to_table()
    rows = sorted(canonical.to_pylist(), key=lambda row: row["event_time"])
    if not rows:
        raise ValueError("evidence validation: empty canonical input")
    bars = tuple(
        ReferenceBar(
            row["event_time"].astimezone(UTC),
            row["open"],
            row["close"],
            row["membership_valid_from"].astimezone(UTC),
            row["membership_valid_to"].astimezone(UTC),
        )
        for row in rows
    )
    reference = calculate_individual_measurements(
        bars, rows[0]["session_open"].astimezone(UTC), rows[0]["session_close"].astimezone(UTC)
    )
    returns = (
        ds.dataset(args.gold_manifest.parent / "returns", format="parquet", partitioning="hive")
        .to_table()
        .to_pylist()
    )
    rolling = (
        ds.dataset(args.gold_manifest.parent / "rolling", format="parquet", partitioning="hive")
        .to_table()
        .to_pylist()
    )
    fixed = (
        ds.dataset(args.gold_manifest.parent / "fixed", format="parquet", partitioning="hive")
        .to_table()
        .to_pylist()
    )
    expected_rolling = {(row.horizon, row.window_end): row for row in reference.rolling}
    expected_fixed = {(row.horizon, row.window_end): row for row in reference.fixed}
    expected_returns = {row.event_time: row for row in reference.returns}
    identity = {
        "symbol": rows[0]["symbol"],
        "source": rows[0]["source"],
        "feed": rows[0]["feed"],
        "timeframe": rows[0]["timeframe"],
        "adjustment": rows[0]["adjustment"],
        "session_id": rows[0]["session_id"],
    }
    max_return_error = 0.0
    max_rolling_error = 0.0
    max_variance_error = 0.0
    max_volatility_error = 0.0
    if len(returns) != len(expected_returns):
        raise ValueError("evidence validation: return record count mismatch")
    for row in returns:
        if any(row[field] != value for field, value in identity.items()):
            raise ValueError("evidence validation: return series identity mismatch")
        expected_return = expected_returns[row["event_time"].astimezone(UTC)]
        if (
            row["return_end"].astimezone(UTC) != expected_return.return_end
            or row["eligible"] != expected_return.eligible
            or row["reason"] != expected_return.reason
            or (row["close_to_close_log_return"] is None)
            != (expected_return.close_to_close_log_return is None)
            or (row["intrabar_log_return"] is None) != (expected_return.intrabar_log_return is None)
        ):
            raise ValueError("evidence validation: return record parity mismatch")
        for actual, expected_value in (
            (row["intrabar_log_return"], expected_return.intrabar_log_return),
            (row["close_to_close_log_return"], expected_return.close_to_close_log_return),
        ):
            if actual is not None and expected_value is not None:
                error = abs(actual - expected_value)
                if error > 1e-12 + 1e-10 * abs(expected_value):
                    raise ValueError("evidence validation: return numerical mismatch")
                max_return_error = max(max_return_error, error)
    if len(rolling) != len(expected_rolling):
        raise ValueError("evidence validation: rolling record count mismatch")
    for row in rolling:
        if any(row[field] != value for field, value in identity.items()):
            raise ValueError("evidence validation: rolling series identity mismatch")
        expected_rolling_window = expected_rolling[
            (row["horizon"], row["window_end"].astimezone(UTC))
        ]
        if (
            row["window_start"].astimezone(UTC) != expected_rolling_window.window_start
            or row["expected_returns"] != expected_rolling_window.expected_returns
            or row["observed_returns"] != expected_rolling_window.observed_returns
            or row["eligible"] != expected_rolling_window.eligible
            or row["reason"] != expected_rolling_window.reason
            or (row["rolling_return_volatility"] is None) != (expected_rolling_window.value is None)
        ):
            raise ValueError("evidence validation: rolling record parity mismatch")
        if row["eligible"]:
            expected_value = expected_rolling_window.value
            assert expected_value is not None
            error = abs(row["rolling_return_volatility"] - expected_value)
            tolerance = 1e-12 + 1e-10 * abs(expected_value)
            if error > tolerance:
                raise ValueError("evidence validation: rolling parity mismatch")
            max_rolling_error = max(max_rolling_error, error)
    if len(fixed) != len(expected_fixed):
        raise ValueError("evidence validation: fixed record count mismatch")
    for row in fixed:
        if any(row[field] != value for field, value in identity.items()):
            raise ValueError("evidence validation: fixed series identity mismatch")
        expected_fixed_window = expected_fixed[(row["horizon"], row["window_end"].astimezone(UTC))]
        if (
            row["window_start"].astimezone(UTC) != expected_fixed_window.window_start
            or row["expected_returns"] != expected_fixed_window.expected_returns
            or row["observed_returns"] != expected_fixed_window.observed_returns
            or row["eligible"] != expected_fixed_window.eligible
            or row["reason"] != expected_fixed_window.reason
            or (row["realized_variance"] is None)
            != (expected_fixed_window.realized_variance is None)
            or (row["realized_volatility"] is None) != (expected_fixed_window.value is None)
        ):
            raise ValueError("evidence validation: fixed record parity mismatch")
        if expected_fixed_window.eligible:
            expected_variance = expected_fixed_window.realized_variance
            expected_volatility = expected_fixed_window.value
            assert expected_variance is not None and expected_volatility is not None
            variance_error = abs(row["realized_variance"] - expected_variance)
            volatility_error = abs(row["realized_volatility"] - expected_volatility)
            variance_tolerance = 1e-12 + 1e-10 * abs(expected_variance)
            volatility_tolerance = 1e-12 + 1e-10 * abs(expected_volatility)
            if variance_error > variance_tolerance or volatility_error > volatility_tolerance:
                raise ValueError("evidence validation: fixed parity mismatch")
            max_variance_error = max(max_variance_error, variance_error)
            max_volatility_error = max(max_volatility_error, volatility_error)
    print(
        json.dumps(
            {
                "silver_artifacts_verified": silver_artifacts,
                "gold_artifacts_verified": gold_artifacts,
                "canonical_records": len(rows),
                "return_records": len(returns),
                "eligible_adjacent_returns": sum(bool(row["eligible"]) for row in returns),
                "rolling_records": len(rolling),
                "eligible_rolling_records": sum(bool(row["eligible"]) for row in rolling),
                "fixed_records": len(fixed),
                "eligible_fixed_records": sum(bool(row["eligible"]) for row in fixed),
                "temporal_and_record_key_mismatches": 0,
                "max_return_absolute_error": max_return_error,
                "max_rolling_absolute_error": max_rolling_error,
                "max_fixed_variance_absolute_error": max_variance_error,
                "max_fixed_volatility_absolute_error": max_volatility_error,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
