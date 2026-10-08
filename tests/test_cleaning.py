from datetime import UTC, datetime
from typing import Any

import pytest

from event_metrics.cleaning import clean_line
from event_metrics.models import CleanEvent, EventType, QualityFlag, RejectedEvent, RejectReason
from tests.factories import MISSING, event_line


def test_valid_event_is_kept_as_is() -> None:
    assert clean_line(event_line()) == CleanEvent(
        event_id="9f1c2b3e",
        event_time=datetime(2025, 1, 12, 10, 32, 14, tzinfo=UTC),
        service="checkout",
        event_type=EventType.REQUEST_COMPLETED,
        latency_ms=123,
        status_code=200,
        user_id="u123",
    )


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"event_id": MISSING}, RejectReason.MISSING_EVENT_ID),
        ({"event_id": "  "}, RejectReason.MISSING_EVENT_ID),
        ({"timestamp": MISSING}, RejectReason.MISSING_TIMESTAMP),
        ({"timestamp": None}, RejectReason.MISSING_TIMESTAMP),
        ({"timestamp": ""}, RejectReason.MISSING_TIMESTAMP),
        ({"timestamp": "2025-13-40T25:61:61Z"}, RejectReason.INVALID_TIMESTAMP),
        ({"service": MISSING}, RejectReason.MISSING_SERVICE),
        ({"service": 42}, RejectReason.MISSING_SERVICE),
        ({"event_type": None}, RejectReason.MISSING_EVENT_TYPE),
        ({"event_type": "request_retried"}, RejectReason.UNKNOWN_EVENT_TYPE),
    ],
)
def test_event_without_usable_required_field_is_rejected(
    overrides: dict[str, Any], reason: RejectReason
) -> None:
    line = event_line(**overrides)

    assert clean_line(line, line_number=7) == RejectedEvent(line.decode(), (reason,), 7)


def test_every_reject_reason_is_reported() -> None:
    result = clean_line(event_line(timestamp=None, service=MISSING))

    assert isinstance(result, RejectedEvent)
    assert result.reasons == (RejectReason.MISSING_TIMESTAMP, RejectReason.MISSING_SERVICE)


@pytest.mark.parametrize(
    ("line", "reason"),
    [
        (b"{not json", RejectReason.INVALID_JSON),
        (b'["a", "list"]', RejectReason.NOT_AN_OBJECT),
        (b'{"event_id": "\xff"}', RejectReason.INVALID_ENCODING),
    ],
)
def test_unparseable_line_is_rejected(line: bytes, reason: RejectReason) -> None:
    result = clean_line(line)

    assert isinstance(result, RejectedEvent)
    assert result.reasons == (reason,)


def test_unusable_optional_fields_are_nulled_and_flagged() -> None:
    result = clean_line(event_line(latency_ms="", status_code="ERR", user_id=MISSING))

    assert isinstance(result, CleanEvent)
    assert (result.latency_ms, result.status_code, result.user_id) == (None, None, None)
    assert result.is_error is None
    assert result.quality_flags == (
        QualityFlag.LATENCY_MISSING,
        QualityFlag.STATUS_CODE_INVALID,
        QualityFlag.USER_ID_MISSING,
    )


def test_numeric_strings_are_coerced_and_flagged() -> None:
    result = clean_line(event_line(latency_ms="123ms", status_code="503"))

    assert isinstance(result, CleanEvent)
    assert (result.latency_ms, result.status_code, result.is_error) == (123, 503, True)
    assert result.quality_flags == (QualityFlag.LATENCY_COERCED, QualityFlag.STATUS_CODE_COERCED)


def test_nonstandard_timestamp_is_normalised_and_flagged() -> None:
    result = clean_line(event_line(timestamp="12/01/2025 10:00:00"))

    assert isinstance(result, CleanEvent)
    assert result.event_time == datetime(2025, 1, 12, 10, tzinfo=UTC)
    assert result.quality_flags == (
        QualityFlag.TIMESTAMP_NONSTANDARD_FORMAT,
        QualityFlag.TIMESTAMP_TIMEZONE_ASSUMED_UTC,
    )


def test_unknown_fields_are_preserved_and_flagged() -> None:
    result = clean_line(event_line(extra_field={"k": "v"}))

    assert isinstance(result, CleanEvent)
    assert result.extra_fields == {"extra_field": {"k": "v"}}
    assert result.quality_flags == (QualityFlag.UNKNOWN_FIELDS,)


def test_failed_event_with_success_status_is_kept_and_flagged() -> None:
    result = clean_line(event_line(event_type="request_failed", status_code=200))

    assert isinstance(result, CleanEvent)
    assert result.is_error is False  # errors are defined by status code, per the metrics spec
    assert result.quality_flags == (QualityFlag.EVENT_TYPE_STATUS_MISMATCH,)


def test_service_and_event_type_are_normalised() -> None:
    result = clean_line(event_line(service=" Checkout ", event_type="REQUEST_COMPLETED"))

    assert isinstance(result, CleanEvent)
    assert (result.service, result.event_type) == ("checkout", EventType.REQUEST_COMPLETED)


def test_clean_event_record_format() -> None:
    result = clean_line(event_line(status_code=503, extra_field=True))

    assert isinstance(result, CleanEvent)
    assert result.to_record() == {
        "event_id": "9f1c2b3e",
        "event_time": "2025-01-12T10:32:14Z",
        "service": "checkout",
        "event_type": "request_completed",
        "latency_ms": 123,
        "status_code": 503,
        "is_error": True,
        "user_id": "u123",
        "quality_flags": ["unknown_fields"],
        "extra_fields": {"extra_field": True},
    }
