"""Batch driver: read raw events, clean, deduplicate, aggregate and write the outputs.

    raw lines ─► clean_line ─┬─► RejectedEvent ─► rejected_events.jsonl   (dead-letter)
                             └─► CleanEvent ─► dedupe on event_id ─► clean_events.jsonl
                                                      └─► 1-minute windows per service
                                                               └─► metrics_per_minute.jsonl

Every stage is a per-element function or a mergeable combiner, so the same code maps onto a
streaming runner (ParDo with a dead-letter output, stateful dedupe, windowed CombinePerKey).
"""

from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from event_metrics.aggregation import ServiceMinuteMetrics, aggregate_per_minute
from event_metrics.cleaning import clean_line
from event_metrics.dedup import FirstSeenDeduplicator
from event_metrics.models import CleanEvent, RejectedEvent

logger = logging.getLogger(__name__)

CLEAN_EVENTS_FILE = "clean_events.jsonl"
REJECTED_EVENTS_FILE = "rejected_events.jsonl"
METRICS_FILE = "metrics_per_minute.jsonl"
SUMMARY_FILE = "run_summary.json"


@dataclass(frozen=True)
class PipelineResult:
    clean_events: list[CleanEvent]
    rejected_events: list[RejectedEvent]
    metrics: list[ServiceMinuteMetrics]
    records_read: int
    duplicates_dropped: int

    def summary(self) -> dict[str, Any]:
        """Data quality report. records_read == clean + rejected + duplicates_dropped."""
        reasons = Counter(r.value for event in self.rejected_events for r in event.reasons)
        flags = Counter(f.value for event in self.clean_events for f in event.quality_flags)
        return {
            "records_read": self.records_read,
            "clean_events": len(self.clean_events),
            "rejected_events": len(self.rejected_events),
            "duplicates_dropped": self.duplicates_dropped,
            "reject_reasons": dict(sorted(reasons.items())),
            "quality_flags": dict(sorted(flags.items())),
            "metric_rows": len(self.metrics),
        }


def process(lines: Iterable[bytes]) -> PipelineResult:
    """Run all stages in memory over raw JSON Lines records. Blank lines are skipped."""
    deduplicator = FirstSeenDeduplicator()
    clean_events: list[CleanEvent] = []
    rejected_events: list[RejectedEvent] = []
    records_read = 0

    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        records_read += 1
        # Validate before deduplicating, so a broken copy of an event never shadows a good one.
        result = clean_line(line, line_number)
        if isinstance(result, RejectedEvent):
            rejected_events.append(result)
        elif deduplicator.accept(result):
            clean_events.append(result)

    return PipelineResult(
        clean_events=clean_events,
        rejected_events=rejected_events,
        metrics=aggregate_per_minute(clean_events),
        records_read=records_read,
        duplicates_dropped=deduplicator.duplicate_count,
    )


def run_pipeline(input_path: Path, output_dir: Path) -> PipelineResult:
    with input_path.open("rb") as source:
        result = process(source)

    output_dir.mkdir(parents=True, exist_ok=True)
    _write_jsonl(output_dir / CLEAN_EVENTS_FILE, (e.to_record() for e in result.clean_events))
    _write_jsonl(output_dir / REJECTED_EVENTS_FILE, (e.to_record() for e in result.rejected_events))
    _write_jsonl(output_dir / METRICS_FILE, (m.to_record() for m in result.metrics))
    _write_text(output_dir / SUMMARY_FILE, json.dumps(result.summary(), indent=2) + "\n")
    return result


def _write_jsonl(path: Path, records: Iterable[dict[str, Any]]) -> None:
    _write_text(path, "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records))


def _write_text(path: Path, content: str) -> None:
    """Write to a temp file and rename, so readers never see a half-written output."""
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(content, encoding="utf-8", newline="\n")
    tmp_path.replace(path)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Clean raw service events and compute per-minute service metrics."
    )
    parser.add_argument("--input", type=Path, default=Path("data/events.jsonl"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/output"))
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = run_pipeline(args.input, args.output_dir)
    logger.info("Wrote outputs to %s: %s", args.output_dir, json.dumps(result.summary()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
