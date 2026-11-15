"""FastAPI webhook server — accepts HTTP webhooks and routes them through ETAA."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from ..engine.event_router import EventRouter
from ..engine.workflow_executor import WorkflowExecutor
from ..ingress import WebhookIngress
from ..observability.logger import setup_logger

log = setup_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("ETAA webhook server starting")
    yield
    log.info("ETAA webhook server shutting down")


def create_app(
    executor: WorkflowExecutor,
    router: EventRouter,
    ingress: WebhookIngress | None = None,
    prefix: str = "/api/v1",
) -> FastAPI:
    """Create and configure the FastAPI application.

    Parameters
    ----------
    executor : WorkflowExecutor
        The central workflow executor.
    router : EventRouter
        Registered workflows live here.
    ingress : WebhookIngress, optional
        Defaults to a fresh WebhookIngress instance.
    prefix : str
        URL prefix for all ETAA routes.
    """
    app = FastAPI(
        title="ETAA — Event-Triggered Automation Agent",
        description="Vendor-agnostic event orchestration layer with idempotent execution, retry logic, and dead-letter handling.",
        version="0.1.0",
        lifespan=lifespan,
    )

    wh_ingress = ingress or WebhookIngress()

    @app.get(f"{prefix}/health")
    async def health() -> dict[str, Any]:
        return {"status": "ok", "workflows_registered": router.workflow_count}

    @app.post(f"{prefix}/webhook/{{source}}")
    async def handle_webhook(source: str, request: Request) -> JSONResponse:
        """Accept a webhook for *source* and dispatch matching workflows."""
        try:
            body = await request.json()
        except Exception:
            body = {}

        # Override source from URL path if not in body
        if "source" not in body:
            body["source"] = source

        event = await wh_ingress.ingest(body)
        runs = await executor.execute(event)

        statuses = [r.status.value for r in runs]
        return JSONResponse(content={"event_id": event.event_id, "runs": statuses})

    @app.post(f"{prefix}/webhook")
    async def handle_any_webhook(request: Request) -> JSONResponse:
        """Generic endpoint for webhooks that include source/type in the payload."""
        try:
            body = await request.json()
        except Exception:
            body = {}
        event = await wh_ingress.ingest(body)
        runs = await executor.execute(event)
        statuses = [r.status.value for r in runs]
        return JSONResponse(content={"event_id": event.event_id, "runs": statuses})

    @app.get(f"{prefix}/workflows")
    async def list_workflows() -> list[Any]:
        return [wf.model_dump() for wf in router.list_workflows()]

    @app.get(f"{prefix}/runs")
    async def list_runs(workflow_id: str | None = None, limit: int = 50) -> list[dict]:
        from ..observability.run_history_store import get_run_history
        hist = get_run_history()
        records = hist.list(workflow_id=workflow_id, limit=limit)
        return [r.model_dump() for r in records]

    @app.get(f"{prefix}/metrics")
    async def get_metrics() -> dict[str, Any]:
        from ..observability.metrics_store import get_metrics_store
        store = get_metrics_store()
        return store.snapshot()

    return app
