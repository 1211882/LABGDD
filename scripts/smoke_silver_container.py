"""Run a synthetic native Spark publication inside the supported Linux image."""

import hashlib
import json
import os
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from uuid import UUID

from labgdd.adapters.bronze import BronzeAcquisition, sha256
from labgdd.config import Settings
from labgdd.processing.silver import process_silver
from labgdd.silver_config import SilverRequest


def main() -> None:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    session_bounds = (
        (
            datetime(2026, 3, 6, 14, 30, tzinfo=UTC),
            datetime(2026, 3, 6, 21, 0, tzinfo=UTC),
        ),
        (
            datetime(2026, 3, 9, 13, 30, tzinfo=UTC),
            datetime(2026, 3, 9, 20, 0, tzinfo=UTC),
        ),
        (
            datetime(2026, 9, 14, 13, 30, tzinfo=UTC),
            datetime(2026, 9, 14, 20, 0, tzinfo=UTC),
        ),
        (
            datetime(2026, 11, 27, 14, 30, tzinfo=UTC),
            datetime(2026, 11, 27, 18, 0, tzinfo=UTC),
        ),
    )
    rows = []
    for number, (event_time, _) in enumerate(session_bounds):
        raw = {"t": event_time.isoformat(), "o": 10, "h": 12, "l": 9, "c": 11, "v": 100}
        rows.append(
            {
                "observation_id": hashlib.sha256(str(number).encode()).hexdigest(),
                "symbol": "AAPL",
                "raw_json": json.dumps(raw, sort_keys=True),
                "event_time": event_time,
                "valid": True,
                "reason": None,
                "session": "regular",
            }
        )
    sessions = [
        {
            "session": opening.date().isoformat(),
            "open": opening.isoformat(),
            "close": closing.isoformat(),
        }
        for opening, closing in session_bounds
    ]
    calendar = {
        "provider": "exchange_calendars",
        "version": "4.11.2",
        "calendar": "XNYS",
        "schedule_sha256": hashlib.sha256(
            json.dumps(sessions, sort_keys=True).encode()
        ).hexdigest(),
        "sessions": sessions,
    }
    acquisition_id = UUID("10000000-0000-0000-0000-000000000001")
    run_id = UUID("20000000-0000-0000-0000-000000000001")
    with tempfile.TemporaryDirectory(prefix="labgdd-smoke-") as temporary:
        root = Path(temporary)
        metadata: dict[str, object] = {
            "manifest_version": "1.0.0",
            "schema_version": "1.0.0",
            "observation_identity_version": "bronze-observation-v1",
            "acquisition_id": str(acquisition_id),
            "dataset_version": "bronze-container-smoke-v1",
            "source": "alpaca",
            "feed": "iex",
            "timeframe": "1Min",
            "adjustment": "raw",
            "symbols": ["AAPL"],
            "calendar": calendar,
            "revision_resolution": "not_performed",
        }
        bronze = BronzeAcquisition(
            root / "data", "bronze-container-smoke-v1", acquisition_id, metadata
        )
        bronze.write_json("calendar.json", calendar)
        bronze.write_parquet(
            "response-000001.parquet",
            [
                {
                    "page": 1,
                    "received_at": datetime(2026, 11, 3, tzinfo=UTC),
                    "request_token": None,
                    "body": b'{"fixture":true}',
                    "body_sha256": hashlib.sha256(b'{"fixture":true}').hexdigest(),
                }
            ],
            pages=True,
        )
        bronze.write_parquet("observations-000001.parquet", rows)
        manifest = bronze.finish("complete", {"revision_resolution": "not_performed"})
        settings = Settings(
            "1.0.0",
            "study-cohort-v1",
            "study-cohort-v1",
            root / "data",
            "INFO",
            MappingProxyType({"Technology": ("AAPL",)}),
        )
        request = SilverRequest(
            "silver-container-smoke-v1",
            (manifest,),
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2027, 1, 1, tzinfo=UTC),
            "synthetic_container_fixture",
        )
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("labgdd-container-smoke")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
            .getOrCreate()
        )
        try:
            from pyspark.sql import DataFrame

            def reject_driver_collection(self: DataFrame) -> list[object]:
                raise AssertionError("native publisher must not collect result frames")

            DataFrame.collect = reject_driver_collection  # type: ignore[method-assign,assignment]
            result = process_silver(settings, request, spark, run_id=run_id)
        finally:
            spark.stop()
        document = json.loads(result.manifest.read_text(encoding="utf-8"))
        if result.status != "complete" or result.counts["canonical_records"] != 4:
            raise RuntimeError("container smoke: unexpected publication result")
        if document["publisher"] != "spark" or document["quarantined_records"] != 0:
            raise RuntimeError("container smoke: unexpected manifest")
        partition_dates = {
            part
            for artifact in document["artifacts"]
            for part in artifact["path"].split("/")
            if part.startswith("session_date=")
        }
        if len(partition_dates) != 4:
            raise RuntimeError("container smoke: expected four physical session partitions")
        for artifact in document["artifacts"]:
            path = result.manifest.parent / artifact["path"]
            if path.stat().st_size != artifact["bytes"] or sha256(path) != artifact["sha256"]:
                raise RuntimeError("container smoke: artifact integrity failure")
        import pyarrow as pa
        import pyarrow.dataset as ds

        canonical = ds.dataset(
            result.manifest.parent / "canonical", format="parquet", partitioning="hive"
        )
        for field in (
            "event_time",
            "bar_end",
            "session_open",
            "session_close",
            "membership_valid_from",
            "membership_valid_to",
        ):
            if canonical.schema.field(field).type != pa.timestamp("us", tz="UTC"):
                raise RuntimeError("container smoke: timestamp physical type mismatch")
        decoded = canonical.to_table().sort_by([("event_time", "ascending")]).to_pylist()
        expected = [
            (opening, opening + timedelta(minutes=1), opening, closing)
            for opening, closing in session_bounds
        ]
        actual = [
            (row["event_time"], row["bar_end"], row["session_open"], row["session_close"])
            for row in decoded
        ]
        if actual != expected:
            raise RuntimeError("container smoke: UTC instant mismatch")
        if any(
            row["membership_valid_from"] != datetime(2026, 1, 1, tzinfo=UTC)
            or row["membership_valid_to"] != datetime(2027, 1, 1, tzinfo=UTC)
            for row in decoded
        ):
            raise RuntimeError("container smoke: membership timestamp mismatch")
        print("container-smoke: complete canonical=4 quarantine=0 sessions=4")


if __name__ == "__main__":
    main()
