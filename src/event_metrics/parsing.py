"""Field-level normalisers.

Each parser takes the raw JSON value of one field and returns the normalised value plus any
quality flags. A `None` value means the field is unusable; the caller decides whether that
rejects the whole event (required fields) or only nulls the field (optional fields).
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Generic, TypeVar

from event_metrics.models import QualityFlag

T = TypeVar("T")

# RFC 3339 date-time. A space instead of `T`, or a missing UTC offset, is tolerated but flagged.
_ISO_DATETIME = re.compile(
    r"\d{4}-\d{2}-\d{2}(?P<sep>[T ])\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})?"
)
# Other layouts seen from producers. "12/01/2025 10:00:00" is read day-first: that gives
# 2025-01-12, the date of every other event in the sample, whereas month-first would place
# these events eleven months after the rest of the stream.
_FALLBACK_TIMESTAMP_FORMATS = ("%d/%m/%Y %H:%M:%S",)

_STATUS_CODE_TEXT = re.compile(r"(?P<value>\d+)")
_LATENCY_TEXT = re.compile(r"(?P<value>\d+)\s*(?:ms)?")  # unit suffix matches the field's unit

# Guards averages against garbage such as an epoch timestamp sent in the latency field.
# Genuine slow requests (the sample has some at ~21s) are well below this and are kept.
MAX_PLAUSIBLE_LATENCY_MS = 60 * 60 * 1000


@dataclass(frozen=True, slots=True)
class FieldResult(Generic[T]):
    value: T | None
    flags: tuple[QualityFlag, ...] = ()


def clean_text(raw: object) -> str | None:
    """Stripped string, or None for absent, non-string or whitespace-only values."""
    if isinstance(raw, str) and (text := raw.strip()):
        return text
    return None


def is_blank(raw: object) -> bool:
    return raw is None or (isinstance(raw, str) and not raw.strip())


def parse_timestamp(raw: object) -> FieldResult[datetime]:
    """Parse an event timestamp into an aware UTC datetime; `value` is None if unparseable."""
    if not isinstance(raw, str):
        return FieldResult(None)
    text = raw.strip()

    if match := _ISO_DATETIME.fullmatch(text):
        parsed = _parse_iso(text)
        nonstandard = match["sep"] != "T"
    else:
        parsed = _parse_fallback(text)
        nonstandard = True
    if parsed is None:
        return FieldResult(None)

    flags = [QualityFlag.TIMESTAMP_NONSTANDARD_FORMAT] if nonstandard else []
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
        flags.append(QualityFlag.TIMESTAMP_TIMEZONE_ASSUMED_UTC)
    return FieldResult(parsed.astimezone(UTC), tuple(flags))


def parse_latency_ms(raw: object) -> FieldResult[int]:
    return _parse_int_field(
        raw,
        text_pattern=_LATENCY_TEXT,
        is_valid=lambda value: 0 <= value <= MAX_PLAUSIBLE_LATENCY_MS,
        missing=QualityFlag.LATENCY_MISSING,
        invalid=QualityFlag.LATENCY_INVALID,
        coerced=QualityFlag.LATENCY_COERCED,
    )


def parse_status_code(raw: object) -> FieldResult[int]:
    return _parse_int_field(
        raw,
        text_pattern=_STATUS_CODE_TEXT,
        is_valid=lambda value: 100 <= value <= 599,  # the HTTP status code range
        missing=QualityFlag.STATUS_CODE_MISSING,
        invalid=QualityFlag.STATUS_CODE_INVALID,
        coerced=QualityFlag.STATUS_CODE_COERCED,
    )


def _parse_iso(text: str) -> datetime | None:
    try:
        return datetime.fromisoformat(text)
    except ValueError:  # well-formed but impossible, e.g. month 13 or hour 25
        return None


def _parse_fallback(text: str) -> datetime | None:
    for fmt in _FALLBACK_TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _parse_int_field(
    raw: object,
    *,
    text_pattern: re.Pattern[str],
    is_valid: Callable[[int], bool],
    missing: QualityFlag,
    invalid: QualityFlag,
    coerced: QualityFlag,
) -> FieldResult[int]:
    if is_blank(raw):
        return FieldResult(None, (missing,))
    value, was_coerced = _to_int(raw, text_pattern)
    if value is None or not is_valid(value):
        return FieldResult(None, (invalid,))
    return FieldResult(value, (coerced,) if was_coerced else ())


def _to_int(raw: object, text_pattern: re.Pattern[str]) -> tuple[int | None, bool]:
    """Return `(value, was_coerced)`; `value` is None when `raw` is not integer-like."""
    if isinstance(raw, bool):  # bool is an int subclass, but `true` is not a status code
        return None, False
    if isinstance(raw, int):
        return raw, False
    if isinstance(raw, float) and raw.is_integer():
        return int(raw), True
    if isinstance(raw, str) and (match := text_pattern.fullmatch(raw.strip())):
        return int(match["value"]), True
    return None, False
