from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError

from event_metrics.api.repository import MetricsRepository
from event_metrics.api.schemas import (
    HealthResponse,
    MetricsResponse,
    MinuteMetricsOut,
    RequestStatsOut,
    ServiceSummaryOut,
    SummaryResponse,
)

router = APIRouter()


def get_repository(request: Request) -> MetricsRepository:
    repository: MetricsRepository = request.app.state.repository
    return repository


Repository = Annotated[MetricsRepository, Depends(get_repository)]


@dataclass(frozen=True)
class MetricsFilter:
    service: str | None
    start: datetime | None
    end: datetime | None


def metrics_filter(
    repository: Repository,
    service: Annotated[
        str | None, Query(description="Service name, e.g. `checkout`. All services if omitted.")
    ] = None,
    start: Annotated[
        datetime | None,
        Query(
            alias="from",
            description="Inclusive lower bound on `window_start`, ISO 8601. Naive = UTC.",
        ),
    ] = None,
    end: Annotated[
        datetime | None,
        Query(alias="to", description="Exclusive upper bound on `window_start`, ISO 8601."),
    ] = None,
) -> MetricsFilter:
    if service is not None:
        service = service.strip().lower()  # the pipeline stores service names lower-cased
        if service not in repository.services:
            known = ", ".join(repository.services)
            raise HTTPException(
                HTTPStatus.NOT_FOUND, f"Unknown service {service!r}. Known services: {known}."
            )
    start, end = _as_utc(start), _as_utc(end)
    if start is not None and end is not None and start >= end:
        # Same error shape as FastAPI's own query validation, so clients handle one format.
        raise RequestValidationError(
            [
                {
                    "type": "value_error",
                    "loc": ("query", "to"),
                    "msg": "Value error, 'to' must be later than 'from'",
                    "input": end.isoformat(),
                }
            ]
        )
    return MetricsFilter(service, start, end)


Filter = Annotated[MetricsFilter, Depends(metrics_filter)]


@router.get("/metrics", summary="Per-minute request metrics")
def list_metrics(query: Filter, repository: Repository) -> MetricsResponse:
    rows = repository.query(query.service, query.start, query.end)
    return MetricsResponse(
        items=[
            MinuteMetricsOut(
                service=row.service,
                window_start=row.window_start,
                **RequestStatsOut.fields_from(row.stats),
            )
            for row in rows
        ]
    )


@router.get("/metrics/summary", summary="Request metrics rolled up over the whole range")
def summarize_metrics(query: Filter, repository: Repository) -> SummaryResponse:
    totals = repository.summarize(query.service, query.start, query.end)
    return SummaryResponse(
        items=[
            ServiceSummaryOut(service=service, **RequestStatsOut.fields_from(stats))
            for service, stats in sorted(totals.items())
        ]
    )


@router.get("/health", summary="Liveness and data freshness")
def health(repository: Repository) -> HealthResponse:
    return HealthResponse(
        services=len(repository.services),
        metric_rows=repository.row_count,
        latest_window=repository.latest_window,
    )


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
