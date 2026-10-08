"""Read access to the per-minute metrics produced by the pipeline."""

from __future__ import annotations

import bisect
import json
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

from event_metrics.aggregation import RequestStats, ServiceMinuteMetrics


class MetricsRepository:
    """In-memory index of per-minute metrics, grouped by service and sorted by window start.

    The sample output easily fits in memory. In production this would sit on Bigtable with a
    `service#window_start` row key, which serves exactly the same per-service range scan.
    """

    def __init__(self, rows: Iterable[ServiceMinuteMetrics]) -> None:
        grouped: defaultdict[str, list[ServiceMinuteMetrics]] = defaultdict(list)
        for row in rows:
            grouped[row.service].append(row)
        self._rows = {
            service: sorted(service_rows, key=lambda row: row.window_start)
            for service, service_rows in grouped.items()
        }
        self._window_starts = {
            service: [row.window_start for row in service_rows]
            for service, service_rows in self._rows.items()
        }

    @classmethod
    def from_jsonl(cls, path: Path) -> MetricsRepository:
        with path.open(encoding="utf-8") as source:
            return cls(
                ServiceMinuteMetrics.from_record(json.loads(line))
                for line in source
                if line.strip()
            )

    @property
    def services(self) -> list[str]:
        return sorted(self._rows)

    @property
    def row_count(self) -> int:
        return sum(len(rows) for rows in self._rows.values())

    @property
    def latest_window(self) -> datetime | None:
        return max((starts[-1] for starts in self._window_starts.values()), default=None)

    def query(
        self,
        service: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[ServiceMinuteMetrics]:
        """Rows with `start <= window_start < end`, for one service or for all of them."""
        result: list[ServiceMinuteMetrics] = []
        for name in [service] if service is not None else self.services:
            rows = self._rows.get(name, [])
            starts = self._window_starts.get(name, [])
            low = 0 if start is None else bisect.bisect_left(starts, start)
            high = len(rows) if end is None else bisect.bisect_left(starts, end)
            result.extend(rows[low:high])
        return result

    def summarize(
        self,
        service: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> dict[str, RequestStats]:
        """Roll the matching minutes up into one exact total per service."""
        totals: dict[str, RequestStats] = {}
        for row in self.query(service, start, end):
            totals.setdefault(row.service, RequestStats()).merge(row.stats)
        return totals
