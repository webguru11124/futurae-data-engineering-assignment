"""Application factory.

Run locally with:
    uvicorn event_metrics.api.app:create_app --factory
"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI

from event_metrics.api.repository import MetricsRepository
from event_metrics.api.routes import router
from event_metrics.pipeline import METRICS_FILE

METRICS_PATH_ENV_VAR = "EVENT_METRICS_PATH"
DEFAULT_METRICS_PATH = Path("data/output") / METRICS_FILE


def create_app(repository: MetricsRepository | None = None) -> FastAPI:
    """Build the app. Without an explicit repository, metrics are loaded from the pipeline
    output at startup; a missing file fails fast instead of serving empty results."""
    if repository is None:
        metrics_path = Path(os.environ.get(METRICS_PATH_ENV_VAR, DEFAULT_METRICS_PATH))
        repository = MetricsRepository.from_jsonl(metrics_path)

    app = FastAPI(
        title="Service metrics API",
        version="0.1.0",
        description="Per-minute request counts, error rates and latencies per service.",
    )
    app.state.repository = repository
    app.include_router(router)
    return app
