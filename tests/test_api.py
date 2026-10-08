import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from event_metrics.aggregation import aggregate_per_minute
from event_metrics.api.app import METRICS_PATH_ENV_VAR, create_app
from event_metrics.api.repository import MetricsRepository
from tests.factories import clean_event

T0 = datetime(2025, 1, 12, 10, 0, tzinfo=UTC)


@pytest.fixture
def client() -> Iterator[TestClient]:
    events = [
        clean_event(event_id="1", event_time=T0, latency_ms=100, status_code=200),
        clean_event(
            event_id="2", event_time=T0 + timedelta(seconds=30), latency_ms=300, status_code=500
        ),
        clean_event(event_id="3", event_time=T0 + timedelta(minutes=1), latency_ms=200),
        clean_event(
            event_id="4", event_time=T0 + timedelta(minutes=2), latency_ms=50, status_code=None
        ),
        clean_event(event_id="5", service="auth", event_time=T0, latency_ms=10, status_code=401),
    ]
    repository = MetricsRepository(aggregate_per_minute(events))
    with TestClient(create_app(repository)) as test_client:
        yield test_client


def test_metrics_for_one_service_in_a_half_open_range(client: TestClient) -> None:
    response = client.get(
        "/metrics",
        params={
            "service": "checkout",
            "from": "2025-01-12T10:00:00Z",
            "to": "2025-01-12T10:02:00Z",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "service": "checkout",
                "window_start": "2025-01-12T10:00:00Z",
                "request_count": 2,
                "error_count": 1,
                "error_rate": 0.5,
                "avg_latency_ms": 200.0,
                "max_latency_ms": 300,
            },
            {
                "service": "checkout",
                "window_start": "2025-01-12T10:01:00Z",
                "request_count": 1,
                "error_count": 0,
                "error_rate": 0.0,
                "avg_latency_ms": 200.0,
                "max_latency_ms": 200,
            },
        ]
    }


def test_without_filters_all_services_are_returned(client: TestClient) -> None:
    items = client.get("/metrics").json()["items"]

    assert [(item["service"], item["window_start"]) for item in items] == [
        ("auth", "2025-01-12T10:00:00Z"),
        ("checkout", "2025-01-12T10:00:00Z"),
        ("checkout", "2025-01-12T10:01:00Z"),
        ("checkout", "2025-01-12T10:02:00Z"),
    ]


def test_service_filter_is_case_insensitive(client: TestClient) -> None:
    upper = client.get("/metrics", params={"service": "CHECKOUT"})

    assert upper.status_code == 200
    assert upper.json() == client.get("/metrics", params={"service": "checkout"}).json()


def test_naive_and_offset_datetimes_are_read_as_utc(client: TestClient) -> None:
    naive = client.get("/metrics", params={"service": "checkout", "from": "2025-01-12T10:01:00"})
    offset = client.get(
        "/metrics", params={"service": "checkout", "from": "2025-01-12T12:01:00+02:00"}
    )

    starts = [item["window_start"] for item in naive.json()["items"]]
    assert starts == ["2025-01-12T10:01:00Z", "2025-01-12T10:02:00Z"]
    assert offset.json() == naive.json()


def test_range_without_data_returns_empty_list(client: TestClient) -> None:
    response = client.get("/metrics", params={"service": "auth", "from": "2025-01-12T11:00:00Z"})

    assert response.status_code == 200
    assert response.json() == {"items": []}


def test_summary_rolls_minutes_up_exactly(client: TestClient) -> None:
    response = client.get("/metrics/summary", params={"service": "checkout"})

    # 4 requests; 3 have a status code, 1 of them an error; latencies 100, 300, 200 and 50.
    assert response.json() == {
        "items": [
            {
                "service": "checkout",
                "request_count": 4,
                "error_count": 1,
                "error_rate": 0.3333,
                "avg_latency_ms": 162.5,
                "max_latency_ms": 300,
            }
        ]
    }


def test_summary_without_service_has_one_item_per_service(client: TestClient) -> None:
    items = client.get("/metrics/summary").json()["items"]

    assert [(item["service"], item["request_count"]) for item in items] == [
        ("auth", 1),
        ("checkout", 4),
    ]


@pytest.mark.parametrize("path", ["/metrics", "/metrics/summary"])
def test_unknown_service_is_404(client: TestClient, path: str) -> None:
    response = client.get(path, params={"service": "billing"})

    assert response.status_code == 404
    assert "Known services: auth, checkout" in response.json()["detail"]


@pytest.mark.parametrize(
    "params",
    [
        {"from": "2025-01-12T10:05:00Z", "to": "2025-01-12T10:00:00Z"},
        {"from": "2025-01-12T10:00:00Z", "to": "2025-01-12T10:00:00Z"},
        {"from": "not-a-date"},
    ],
)
def test_invalid_range_is_422_in_fastapi_error_format(
    client: TestClient, params: dict[str, str]
) -> None:
    response = client.get("/metrics", params=params)

    assert response.status_code == 422
    [error] = response.json()["detail"]
    assert error["loc"][0] == "query"


def test_health_reports_data_freshness(client: TestClient) -> None:
    assert client.get("/health").json() == {
        "status": "ok",
        "services": 2,
        "metric_rows": 4,
        "latest_window": "2025-01-12T10:02:00Z",
    }


def test_app_loads_metrics_from_configured_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    [row] = aggregate_per_minute([clean_event()])
    metrics_path = tmp_path / "metrics.jsonl"
    metrics_path.write_text(json.dumps(row.to_record()) + "\n", encoding="utf-8")
    monkeypatch.setenv(METRICS_PATH_ENV_VAR, str(metrics_path))

    with TestClient(create_app()) as test_client:
        assert test_client.get("/health").json()["metric_rows"] == 1


def test_app_fails_fast_without_metrics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(METRICS_PATH_ENV_VAR, str(tmp_path / "missing.jsonl"))

    with pytest.raises(FileNotFoundError):
        create_app()
