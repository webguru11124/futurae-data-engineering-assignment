"""Record-level validation: one raw input line in, a CleanEvent or a RejectedEvent out.

Drop vs keep policy:
* Reject (dead-letter) when a field needed to place the event in the metrics is unusable:
  `event_id` (dedup key), `timestamp` (time window), `service` (grouping key) and
  `event_type` (decides whether the event is a request at all).
* Keep, null the field and flag it, when an optional measurement is unusable: `latency_ms`,
  `status_code` and `user_id`. The event still counts as a request; it is only left out of
  the averages / rates that need the missing value.

This is a pure per-element function, i.e. a ParDo with a dead-letter output in Beam.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from event_metrics.models import (
    CleanEvent,
    EventType,
    QualityFlag,
    RejectedEvent,
    RejectReason,
    is_error_status,
)
from event_metrics.parsing import (
    clean_text,
    is_blank,
    parse_latency_ms,
    parse_status_code,
    parse_timestamp,
)

KNOWN_FIELDS = frozenset(
    {"event_id", "timestamp", "service", "event_type", "latency_ms", "status_code", "user_id"}
)


def clean_line(line: bytes, line_number: int | None = None) -> CleanEvent | RejectedEvent:
    """Decode, parse and validate one JSON Lines record."""
    try:
        text = line.decode("utf-8").strip()
    except UnicodeDecodeError:
        raw = line.decode("utf-8", errors="replace").strip()
        return RejectedEvent(raw, (RejectReason.INVALID_ENCODING,), line_number)

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return RejectedEvent(text, (RejectReason.INVALID_JSON,), line_number)
    if not isinstance(payload, dict):
        return RejectedEvent(text, (RejectReason.NOT_AN_OBJECT,), line_number)

    result = clean_payload(payload)
    if isinstance(result, CleanEvent):
        return result
    return RejectedEvent(text, result, line_number)


def clean_payload(payload: Mapping[str, Any]) -> CleanEvent | tuple[RejectReason, ...]:
    """Validate a decoded event. Returns every reject reason found, not just the first."""
    reasons: list[RejectReason] = []

    event_id = clean_text(payload.get("event_id"))
    if event_id is None:
        reasons.append(RejectReason.MISSING_EVENT_ID)

    raw_timestamp = payload.get("timestamp")
    timestamp = parse_timestamp(raw_timestamp)
    if is_blank(raw_timestamp):
        reasons.append(RejectReason.MISSING_TIMESTAMP)
    elif timestamp.value is None:
        reasons.append(RejectReason.INVALID_TIMESTAMP)

    service = clean_text(payload.get("service"))
    if service is None:
        reasons.append(RejectReason.MISSING_SERVICE)

    event_type_text = clean_text(payload.get("event_type"))
    event_type = _parse_event_type(event_type_text)
    if event_type_text is None:
        reasons.append(RejectReason.MISSING_EVENT_TYPE)
    elif event_type is None:
        reasons.append(RejectReason.UNKNOWN_EVENT_TYPE)

    if event_id is None or timestamp.value is None or service is None or event_type is None:
        return tuple(reasons)

    latency = parse_latency_ms(payload.get("latency_ms"))
    status = parse_status_code(payload.get("status_code"))
    user_id = clean_text(payload.get("user_id"))
    extra_fields = {key: value for key, value in payload.items() if key not in KNOWN_FIELDS}

    flags = [*timestamp.flags, *latency.flags, *status.flags]
    if user_id is None:
        flags.append(QualityFlag.USER_ID_MISSING)
    if (
        event_type is EventType.REQUEST_FAILED
        and status.value is not None
        and not is_error_status(status.value)
    ):
        # Kept as-is: the metrics spec defines errors by status code, not by event type.
        flags.append(QualityFlag.EVENT_TYPE_STATUS_MISMATCH)
    if extra_fields:
        flags.append(QualityFlag.UNKNOWN_FIELDS)

    return CleanEvent(
        event_id=event_id,
        event_time=timestamp.value,
        service=service.lower(),
        event_type=event_type,
        latency_ms=latency.value,
        status_code=status.value,
        user_id=user_id,
        quality_flags=tuple(flags),
        extra_fields=extra_fields,
    )


def _parse_event_type(text: str | None) -> EventType | None:
    if text is None:
        return None
    try:
        return EventType(text.lower())
    except ValueError:
        return None
