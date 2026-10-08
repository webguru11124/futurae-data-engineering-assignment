from datetime import UTC, datetime, timedelta

import pytest

from event_metrics.models import QualityFlag
from event_metrics.parsing import (
    MAX_PLAUSIBLE_LATENCY_MS,
    parse_latency_ms,
    parse_status_code,
    parse_timestamp,
)

NONSTANDARD = QualityFlag.TIMESTAMP_NONSTANDARD_FORMAT
ASSUMED_UTC = QualityFlag.TIMESTAMP_TIMEZONE_ASSUMED_UTC


@pytest.mark.parametrize(
    ("raw", "expected", "flags"),
    [
        ("2025-01-12T10:32:14Z", datetime(2025, 1, 12, 10, 32, 14, tzinfo=UTC), ()),
        ("2025-01-12T10:32:14.25Z", datetime(2025, 1, 12, 10, 32, 14, 250000, tzinfo=UTC), ()),
        ("2025-01-12T12:32:14+02:00", datetime(2025, 1, 12, 10, 32, 14, tzinfo=UTC), ()),
        (" 2025-01-12T10:32:14Z ", datetime(2025, 1, 12, 10, 32, 14, tzinfo=UTC), ()),
        ("2025-01-12 10:00:00Z", datetime(2025, 1, 12, 10, tzinfo=UTC), (NONSTANDARD,)),
        ("2025-01-12T10:00:00", datetime(2025, 1, 12, 10, tzinfo=UTC), (ASSUMED_UTC,)),
        # Day-first: the only reading consistent with the rest of the stream (2025-01-12).
        ("12/01/2025 10:00:00", datetime(2025, 1, 12, 10, tzinfo=UTC), (NONSTANDARD, ASSUMED_UTC)),
    ],
)
def test_parse_timestamp_normalises_to_utc(
    raw: str, expected: datetime, flags: tuple[QualityFlag, ...]
) -> None:
    result = parse_timestamp(raw)

    assert result.value == expected
    assert result.value is not None and result.value.utcoffset() == timedelta(0)
    assert result.flags == flags


@pytest.mark.parametrize(
    "raw",
    [
        "2025-13-40T25:61:61Z",  # right shape, impossible values (seen in the sample)
        "2025-02-30T10:00:00Z",
        "2025-01-12",  # date only: too coarse to place in a minute window
        "yesterday",
        "1736676734",
        1736676734,
        "",
        None,
    ],
)
def test_parse_timestamp_rejects_unusable_values(raw: object) -> None:
    assert parse_timestamp(raw).value is None


@pytest.mark.parametrize(
    ("raw", "value", "flags"),
    [
        (123, 123, ()),
        (0, 0, ()),
        (21460, 21460, ()),  # slow but real: outliers are signal, not noise
        ("123ms", 123, (QualityFlag.LATENCY_COERCED,)),
        ("123", 123, (QualityFlag.LATENCY_COERCED,)),
        (123.0, 123, (QualityFlag.LATENCY_COERCED,)),
        (None, None, (QualityFlag.LATENCY_MISSING,)),
        ("", None, (QualityFlag.LATENCY_MISSING,)),
        ("  ", None, (QualityFlag.LATENCY_MISSING,)),
        (-5, None, (QualityFlag.LATENCY_INVALID,)),
        ("-5ms", None, (QualityFlag.LATENCY_INVALID,)),
        (12.5, None, (QualityFlag.LATENCY_INVALID,)),
        (True, None, (QualityFlag.LATENCY_INVALID,)),
        ("fast", None, (QualityFlag.LATENCY_INVALID,)),
        (MAX_PLAUSIBLE_LATENCY_MS + 1, None, (QualityFlag.LATENCY_INVALID,)),
    ],
)
def test_parse_latency_ms(raw: object, value: int | None, flags: tuple[QualityFlag, ...]) -> None:
    result = parse_latency_ms(raw)

    assert (result.value, result.flags) == (value, flags)


@pytest.mark.parametrize(
    ("raw", "value", "flags"),
    [
        (200, 200, ()),
        (100, 100, ()),
        (599, 599, ()),
        ("200", 200, (QualityFlag.STATUS_CODE_COERCED,)),
        (None, None, (QualityFlag.STATUS_CODE_MISSING,)),
        ("ERR", None, (QualityFlag.STATUS_CODE_INVALID,)),
        (0, None, (QualityFlag.STATUS_CODE_INVALID,)),
        (700, None, (QualityFlag.STATUS_CODE_INVALID,)),
        (True, None, (QualityFlag.STATUS_CODE_INVALID,)),
        ("200ms", None, (QualityFlag.STATUS_CODE_INVALID,)),
    ],
)
def test_parse_status_code(raw: object, value: int | None, flags: tuple[QualityFlag, ...]) -> None:
    result = parse_status_code(raw)

    assert (result.value, result.flags) == (value, flags)
