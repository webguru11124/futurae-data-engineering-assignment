import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from event_metrics.api.repository import MetricsRepository
from event_metrics.models import TERMINAL_EVENT_TYPES
from event_metrics.pipeline import (
    CLEAN_EVENTS_FILE,
    METRICS_FILE,
    REJECTED_EVENTS_FILE,
    SUMMARY_FILE,
    process,
    run_pipeline,
)
from tests.factories import MISSING, event_line

SAMPLE_DATASET = Path(__file__).parents[1] / "data" / "events.jsonl"


def test_every_record_is_accounted_for() -> None:
    lines = [
        event_line(event_id="a"),
        event_line(event_id="a", timestamp="2025-01-12T10:32:16Z"),  # redelivered, re-stamped
        event_line(event_id="b", timestamp=None),
        b"\n",  # blank lines are not records
        b"{broken",
        event_line(event_id="c", event_type="request_started"),
    ]

    result = process(lines)

    assert result.records_read == 5
    assert [event.event_id for event in result.clean_events] == ["a", "c"]
    assert result.clean_events[0].event_time == datetime(2025, 1, 12, 10, 32, 14, tzinfo=UTC)
    assert result.duplicates_dropped == 1
    assert [event.line_number for event in result.rejected_events] == [3, 5]
    assert result.summary()["reject_reasons"] == {"invalid_json": 1, "missing_timestamp": 1}
    assert [row.stats.request_count for row in result.metrics] == [1]  # "c" is request_started


def test_rejected_copy_does_not_shadow_a_valid_copy() -> None:
    result = process([event_line(event_id="a", timestamp="garbage"), event_line(event_id="a")])

    assert [event.event_id for event in result.clean_events] == ["a"]
    assert result.duplicates_dropped == 0


def test_run_pipeline_writes_outputs_the_api_can_read(tmp_path: Path) -> None:
    input_path = tmp_path / "events.jsonl"
    input_path.write_bytes(event_line(event_id="a") + b"\n" + event_line(service=MISSING) + b"\n")
    output_dir = tmp_path / "output"

    run_pipeline(input_path, output_dir)

    assert sorted(path.name for path in output_dir.iterdir()) == sorted(
        [CLEAN_EVENTS_FILE, REJECTED_EVENTS_FILE, METRICS_FILE, SUMMARY_FILE]
    )
    summary = json.loads((output_dir / SUMMARY_FILE).read_text(encoding="utf-8"))
    assert (summary["clean_events"], summary["rejected_events"]) == (1, 1)
    repository = MetricsRepository.from_jsonl(output_dir / METRICS_FILE)
    assert repository.services == ["checkout"]


@pytest.mark.skipif(not SAMPLE_DATASET.exists(), reason="sample dataset not present")
def test_sample_dataset_regression() -> None:
    with SAMPLE_DATASET.open("rb") as source:
        result = process(source)
    summary = result.summary()

    # Expected counts were cross-checked against an independent profile of the raw file.
    assert summary["records_read"] == 2020
    assert summary["duplicates_dropped"] == 20
    assert summary["reject_reasons"] == {
        "invalid_timestamp": 6,
        "missing_service": 7,
        "missing_timestamp": 24,
    }
    assert summary["records_read"] == (
        summary["clean_events"] + summary["rejected_events"] + summary["duplicates_dropped"]
    )
    assert len({event.event_id for event in result.clean_events}) == len(result.clean_events)
    terminal_events = [e for e in result.clean_events if e.event_type in TERMINAL_EVENT_TYPES]
    assert sum(row.stats.request_count for row in result.metrics) == len(terminal_events)
