"""Shared fixtures for the ETAA test suite (offline, in-memory stores)."""

from __future__ import annotations

from typing import Any

import pytest

from etaa.actions import AiAction, FunctionAction, HttpAction, QueueAction
from etaa.engine.event_router import EventRouter
from etaa.engine.idempotency_manager import IdempotencyManager
from etaa.engine.workflow_executor import WorkflowExecutor
from etaa.ingress.webhook_server import create_app
from etaa.observability.metrics_store import MetricsStore
from etaa.observability.run_history_store import RunHistoryStore


@pytest.fixture
def app_env(tmp_path):
    """Build a fresh FastAPI app wired to isolated in-memory stores."""
    router = EventRouter()
    registry: dict[str, Any] = {
        "http": HttpAction(),
        "queue": QueueAction(),
        "function": FunctionAction(),
        "ai": AiAction(),
    }
    executor = WorkflowExecutor(
        router,
        registry.get,
        MetricsStore(),
        RunHistoryStore(path=str(tmp_path / "runs")),
        IdempotencyManager(ttl_seconds=60),
    )
    app = create_app(executor, router)
    return app, router, executor
