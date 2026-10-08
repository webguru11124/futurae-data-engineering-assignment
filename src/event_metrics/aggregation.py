"""Per-service, per-minute request metrics.

`RequestStats` only holds counts, sums and a max, so two instances merge exactly. That is what
lets the per-minute rows be rolled up to any longer range without averaging averages, and it is
the same create/add/merge/extract contract as a Beam CombineFn (which is how the aggregation would
run in Dataflow: 1-minute fixed event-time windows, keyed by service).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any

from event_metrics.models import TERMINAL_EVENT_TYPES, CleanEvent, format_utc, is_error_status


@dataclass(slots=True)
class RequestStats:
    request_count: int = 0
    error_count: int = 0
    # Requests with a usable status code: the error-rate denominator.
    status_code_count: int = 0
    # Requests with a usable latency: the average-latency denominator.
    latency_count: int = 0
    latency_sum_ms: int = 0
    max_latency_ms: int | None = None

    def add(self, event: CleanEvent) -> None:
        self.request_count += 1
        if event.status_code is not None:
            self.status_code_count += 1
            if is_error_status(event.status_code):
                self.error_count += 1
        if event.latency_ms is not None:
            self.latency_count += 1
            self.latency_sum_ms += event.latency_ms
            self.max_latency_ms = _max_or_none(self.max_latency_ms, event.latency_ms)

    def merge(self, other: RequestStats) -> None:
        self.request_count += other.request_count
        self.error_count += other.error_count
        self.status_code_count += other.status_code_count
        self.latency_count += other.latency_count
        self.latency_sum_ms += other.latency_sum_ms
        self.max_latency_ms = _max_or_none(self.max_latency_ms, other.max_latency_ms)

    @property
    def error_rate(self) -> float | None:
        if not self.status_code_count:
            return None
        return round(self.error_count / self.status_code_count, 4)

    @property
    def avg_latency_ms(self) -> float | None:
        if not self.latency_count:
            return None
        return round(self.latency_sum_ms / self.latency_count, 2)

    def to_record(self) -> dict[str, Any]:
        return {
            "request_count": self.request_count,
            "error_count": self.error_count,
            "error_rate": self.error_rate,
            "avg_latency_ms": self.avg_latency_ms,
            "max_latency_ms": self.max_latency_ms,
            "status_code_count": self.status_code_count,
            "latency_count": self.latency_count,
            "latency_sum_ms": self.latency_sum_ms,
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> RequestStats:
        """Rebuild from the additive fields only; derived rates are recomputed, never trusted."""
        return cls(**{f.name: record[f.name] for f in fields(cls)})


@dataclass(frozen=True, slots=True)
class ServiceMinuteMetrics:
    service: str
    window_start: datetime  # timezone-aware UTC, truncated to the minute
    stats: RequestStats

    def to_record(self) -> dict[str, Any]:
        return {
            "service": self.service,
            "window_start": format_utc(self.window_start),
            **self.stats.to_record(),
        }

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> ServiceMinuteMetrics:
        return cls(
            service=record["service"],
            window_start=datetime.fromisoformat(record["window_start"]),
            stats=RequestStats.from_record(record),
        )


def minute_window(event_time: datetime) -> datetime:
    return event_time.replace(second=0, microsecond=0)


def aggregate_per_minute(events: Iterable[CleanEvent]) -> list[ServiceMinuteMetrics]:
    """Aggregate terminal events into per-(service, minute) stats, ordered by service and time.

    Input order does not matter, so out-of-order events land in the right window. Minutes with
    no requests produce no row, as a streaming window with no elements would.
    """
    windows: defaultdict[tuple[str, datetime], RequestStats] = defaultdict(RequestStats)
    for event in events:
        if event.event_type in TERMINAL_EVENT_TYPES:
            windows[(event.service, minute_window(event.event_time))].add(event)
    return [
        ServiceMinuteMetrics(service, window_start, stats)
        for (service, window_start), stats in sorted(windows.items())
    ]


def _max_or_none(current: int | None, candidate: int | None) -> int | None:
    if current is None:
        return candidate
    if candidate is None:
        return current
    return max(current, candidate)
