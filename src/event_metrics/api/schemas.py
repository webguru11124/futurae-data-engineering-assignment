"""Response models. Kept separate from the pipeline dataclasses so the API contract can evolve
(or stay stable) independently of the storage format."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from event_metrics.aggregation import RequestStats


class ServiceKey(BaseModel):
    service: str


class MinuteKey(ServiceKey):
    window_start: datetime = Field(description="Start of the one-minute UTC window.")


class RequestStatsOut(BaseModel):
    request_count: int = Field(description="Requests that completed or failed in the period.")
    error_count: int = Field(description="Requests whose status code is outside 200-299.")
    error_rate: float | None = Field(
        description="error_count over requests with a valid status code; null if there were none."
    )
    avg_latency_ms: float | None = Field(
        description="Mean latency over requests with a valid latency; null if there were none."
    )
    max_latency_ms: int | None

    @staticmethod
    def fields_from(stats: RequestStats) -> dict[str, Any]:
        return {
            "request_count": stats.request_count,
            "error_count": stats.error_count,
            "error_rate": stats.error_rate,
            "avg_latency_ms": stats.avg_latency_ms,
            "max_latency_ms": stats.max_latency_ms,
        }


# The key class is listed last so its fields come first in the JSON: pydantic orders fields
# starting from the most basic class in the MRO.
class MinuteMetricsOut(RequestStatsOut, MinuteKey):
    pass


class ServiceSummaryOut(RequestStatsOut, ServiceKey):
    pass


class MetricsResponse(BaseModel):
    items: list[MinuteMetricsOut]


class SummaryResponse(BaseModel):
    items: list[ServiceSummaryOut]


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"
    services: int
    metric_rows: int
    latest_window: datetime | None = Field(description="Most recent window loaded (freshness).")
