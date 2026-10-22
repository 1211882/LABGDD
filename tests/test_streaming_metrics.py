from __future__ import annotations

from src.metrics.streaming_query_metrics import StructuredStreamingMetrics


def test_phase5_metrics_summarize_windows_and_watermark_drops() -> None:
    metrics = StructuredStreamingMetrics(
        kafka_topic="bars",
        watermark_delay="10 minutes",
        checkpoint_path="checkpoints",
        output_path="valid",
        invalid_output_path="invalid",
        connector_package="connector",
    )
    metrics.record_analytics(
        "symbol_analytics_5_minutes", 1, 3, ["5 minutes"], ["AAPL"], ["Technology"],
        {"total_windows": 3, "complete_windows": 2, "incomplete_windows": 1},
    )
    metrics.record_analytics(
        "sector_analytics_5_minutes", 1, 1, ["5 minutes"], [], ["Technology"]
    )
    metrics.finish(
        {
            "valid": [
                {
                    "numInputRows": 4,
                    "stateOperators": [
                        {
                            "numRowsDroppedByWatermark": 2,
                            "customMetrics": {"numDroppedDuplicateRows": 1},
                        }
                    ],
                }
            ]
        },
        status="completed",
    )

    assert metrics.analytical_windows_per_duration == {
        "5 minutes": {"symbol": 3, "sector": 1}
    }
    assert metrics.analytics_coverage_by_query["symbol_analytics_5_minutes"]["incomplete_windows"] == 1
    assert metrics.input_events == 4
    assert metrics.duplicate_events == 1
    assert metrics.late_events == 2
    assert metrics.dropped_watermark_events == 2
