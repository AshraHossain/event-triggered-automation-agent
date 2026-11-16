"""FastAPI webhook server — accepts HTTP webhooks and routes them through ETAA."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from ..engine.event_router import EventRouter
from ..engine.workflow_executor import WorkflowExecutor
from ..ingress import WebhookIngress, verify_signature, webhook_secret
from ..observability.logger import setup_logger

log = setup_logger(__name__)

SIGNATURE_HEADER = "X-ETAA-Signature"


async def _read_body(request: Request, source: str) -> dict[str, Any]:
    """Read, verify, and parse a webhook request body.

    Raises:
        HTTPException: 400 on malformed JSON, 401 on a bad or missing signature
            when a secret is configured for *source*.
    """
    raw = await request.body()

    secret = webhook_secret(source)
    if secret is not None:
        signature = request.headers.get(SIGNATURE_HEADER)
        if not verify_signature(secret, raw, signature):
            log.warning("Rejected webhook for source=%s: invalid signature", source)
            raise HTTPException(status_code=401, detail="Invalid webhook signature")

    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=400, detail="Request body is not valid JSON") from exc
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")
    return data


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
        description=(
            "Vendor-agnostic event orchestration layer with idempotent "
            "execution, retry logic, and dead-letter handling."
        ),
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
        body = await _read_body(request, source)
        body.setdefault("source", source)  # URL path is the source of record
        event = await wh_ingress.ingest(body)
        runs = await executor.execute(event)
        statuses = [r.status.value for r in runs]
        return JSONResponse(content={"event_id": event.event_id, "runs": statuses})

    @app.post(f"{prefix}/webhook")
    async def handle_any_webhook(request: Request) -> JSONResponse:
        """Generic endpoint for webhooks that include source/type in the payload."""
        # Peek at the declared source to select the signing secret.
        peek = await request.body()
        try:
            declared_source = str(json.loads(peek).get("source", "")) if peek else ""
        except (json.JSONDecodeError, UnicodeDecodeError):
            declared_source = ""
        body = await _read_body(request, declared_source)
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
