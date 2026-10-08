"""Builders for test events. Every builder starts from one valid event and applies overrides."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from event_metrics.models import CleanEvent, EventType

MISSING: Any = object()  # override value that removes the key from the raw event

VALID_RAW_EVENT: dict[str, Any] = {
    "event_id": "9f1c2b3e",
    "timestamp": "2025-01-12T10:32:14Z",
    "service": "checkout",
    "event_type": "request_completed",
    "latency_ms": 123,
    "status_code": 200,
    "user_id": "u123",
}


def raw_event(**overrides: Any) -> dict[str, Any]:
    event = {**VALID_RAW_EVENT, **overrides}
    return {key: value for key, value in event.items() if value is not MISSING}


def event_line(**overrides: Any) -> bytes:
    return json.dumps(raw_event(**overrides)).encode("utf-8")


def clean_event(**overrides: Any) -> CleanEvent:
    event = CleanEvent(
        event_id="9f1c2b3e",
        event_time=datetime(2025, 1, 12, 10, 32, 14, tzinfo=UTC),
        service="checkout",
        event_type=EventType.REQUEST_COMPLETED,
        latency_ms=123,
        status_code=200,
        user_id="u123",
    )
    return replace(event, **overrides)
