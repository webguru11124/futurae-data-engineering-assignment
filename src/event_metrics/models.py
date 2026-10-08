"""Domain types shared by the pipeline stages and the API."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class EventType(StrEnum):
    REQUEST_STARTED = "request_started"
    REQUEST_COMPLETED = "request_completed"
    REQUEST_FAILED = "request_failed"


# A request is counted once, when it reaches a terminal state. `request_started` carries no
# outcome, so counting it as well would double count every request that emits both events.
TERMINAL_EVENT_TYPES: frozenset[EventType] = frozenset(
    {EventType.REQUEST_COMPLETED, EventType.REQUEST_FAILED}
)


class RejectReason(StrEnum):
    """Why an event could not be used at all. Rejected events go to the dead-letter output."""

    INVALID_ENCODING = "invalid_encoding"
    INVALID_JSON = "invalid_json"
    NOT_AN_OBJECT = "not_an_object"
    MISSING_EVENT_ID = "missing_event_id"
    MISSING_TIMESTAMP = "missing_timestamp"
    INVALID_TIMESTAMP = "invalid_timestamp"
    MISSING_SERVICE = "missing_service"
    MISSING_EVENT_TYPE = "missing_event_type"
    UNKNOWN_EVENT_TYPE = "unknown_event_type"


class QualityFlag(StrEnum):
    """A recoverable problem found on an event that was kept."""

    TIMESTAMP_NONSTANDARD_FORMAT = "timestamp_nonstandard_format"
    TIMESTAMP_TIMEZONE_ASSUMED_UTC = "timestamp_timezone_assumed_utc"
    LATENCY_MISSING = "latency_missing"
    LATENCY_INVALID = "latency_invalid"
    LATENCY_COERCED = "latency_coerced"
    STATUS_CODE_MISSING = "status_code_missing"
    STATUS_CODE_INVALID = "status_code_invalid"
    STATUS_CODE_COERCED = "status_code_coerced"
    USER_ID_MISSING = "user_id_missing"
    EVENT_TYPE_STATUS_MISMATCH = "event_type_status_mismatch"
    UNKNOWN_FIELDS = "unknown_fields"


def is_error_status(status_code: int) -> bool:
    """Error definition from the metrics spec: any status outside 200-299 (so 3xx counts too)."""
    return not 200 <= status_code <= 299


def format_utc(value: datetime) -> str:
    """Render a timezone-aware datetime as ISO 8601 UTC with a `Z` suffix."""
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class CleanEvent:
    event_id: str
    event_time: datetime  # timezone-aware, UTC
    service: str
    event_type: EventType
    latency_ms: int | None
    status_code: int | None
    user_id: str | None
    quality_flags: tuple[QualityFlag, ...] = ()
    # Fields outside the known schema, kept verbatim so schema drift is visible and recoverable.
    extra_fields: dict[str, Any] = field(default_factory=dict)

    @property
    def is_error(self) -> bool | None:
        return None if self.status_code is None else is_error_status(self.status_code)

    def to_record(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_time": format_utc(self.event_time),
            "service": self.service,
            "event_type": self.event_type.value,
            "latency_ms": self.latency_ms,
            "status_code": self.status_code,
            "is_error": self.is_error,
            "user_id": self.user_id,
            "quality_flags": [flag.value for flag in self.quality_flags],
            "extra_fields": self.extra_fields,
        }


@dataclass(frozen=True, slots=True)
class RejectedEvent:
    raw: str
    reasons: tuple[RejectReason, ...]
    line_number: int | None = None

    def to_record(self) -> dict[str, Any]:
        return {
            "line_number": self.line_number,
            "reasons": [reason.value for reason in self.reasons],
            "raw": self.raw,
        }
