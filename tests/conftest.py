"""Shared fixtures for the ETAA test suite (offline, in-memory stores)."""

from __future__ import annotations

from typing import Any

import pytest

from etaa.actions import AiAction, FunctionAction, HttpAction, QueueAction
from etaa.engine.event_router import EventRouter
from etaa.engine.idempotency_manager import IdempotencyManager
from etaa.engine.retry_dlq_manager import RetryDLQManager
from etaa.engine.workflow_executor import WorkflowExecutor
from etaa.ingress.webhook_server import create_app
from etaa.observability.metrics_store import MetricsStore
from etaa.observability.run_history_store import RunHistoryStore


@pytest.fixture
def make_executor(tmp_path):
    """Factory for an executor on isolated stores; optionally with a custom HTTP transport."""

    def _make(transport=None) -> tuple[WorkflowExecutor, EventRouter, dict[str, Any]]:
        router = EventRouter()
        registry: dict[str, Any] = {
            "http": HttpAction(transport=transport),
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
            RetryDLQManager(sleep=_no_sleep),
        )
        return executor, router, registry

    return _make


async def _no_sleep(_seconds: float) -> None:
    """Skip real backoff delays so retry tests run instantly."""


@pytest.fixture
def app_env(make_executor):
    """Build a fresh FastAPI app wired to isolated in-memory stores."""
    executor, router, _ = make_executor()
    return create_app(executor, router), router, executor
