from datetime import UTC, datetime, timedelta

from event_metrics.aggregation import RequestStats, ServiceMinuteMetrics, aggregate_per_minute
from event_metrics.models import EventType
from tests.factories import clean_event

T0 = datetime(2025, 1, 12, 10, 0, tzinfo=UTC)


def test_events_are_bucketed_by_service_and_minute_regardless_of_arrival_order() -> None:
    events = [
        clean_event(service="checkout", event_time=T0 + timedelta(seconds=59)),
        clean_event(service="checkout", event_time=T0 + timedelta(minutes=1)),
        clean_event(service="auth", event_time=T0 + timedelta(seconds=5)),
        clean_event(service="checkout", event_time=T0),
    ]

    metrics = aggregate_per_minute(events)

    assert [(m.service, m.window_start, m.stats.request_count) for m in metrics] == [
        ("auth", T0, 1),
        ("checkout", T0, 2),
        ("checkout", T0 + timedelta(minutes=1), 1),
    ]


def test_only_terminal_events_count_as_requests() -> None:
    events = [clean_event(event_type=event_type) for event_type in EventType]

    [row] = aggregate_per_minute(events)

    assert row.stats.request_count == 2  # completed + failed; started has no outcome yet


def test_stats_use_only_usable_values_as_denominators() -> None:
    stats = RequestStats()
    for status_code, latency_ms in [(200, 100), (204, None), (301, 300), (503, 200), (None, 400)]:
        stats.add(clean_event(status_code=status_code, latency_ms=latency_ms))

    assert stats == RequestStats(
        request_count=5,
        error_count=2,  # 301 and 503: anything outside 200-299 is an error
        status_code_count=4,
        latency_count=4,
        latency_sum_ms=1000,
        max_latency_ms=400,
    )
    assert stats.error_rate == 0.5
    assert stats.avg_latency_ms == 250.0


def test_rates_are_none_when_nothing_could_be_measured() -> None:
    stats = RequestStats()
    stats.add(clean_event(status_code=None, latency_ms=None))

    assert stats.request_count == 1
    assert (stats.error_rate, stats.avg_latency_ms, stats.max_latency_ms) == (None, None, None)


def test_merge_gives_exact_totals_not_an_average_of_averages() -> None:
    busy_minute = RequestStats()
    for _ in range(9):
        busy_minute.add(clean_event(latency_ms=100, status_code=200))
    quiet_minute = RequestStats()
    quiet_minute.add(clean_event(latency_ms=1000, status_code=500))

    busy_minute.merge(quiet_minute)

    assert busy_minute.avg_latency_ms == 190.0  # (9 * 100 + 1000) / 10, not (100 + 1000) / 2
    assert busy_minute.error_rate == 0.1
    assert busy_minute.max_latency_ms == 1000


def test_metrics_record_round_trip() -> None:
    [row] = aggregate_per_minute([clean_event(), clean_event(status_code=None)])

    assert ServiceMinuteMetrics.from_record(row.to_record()) == row
