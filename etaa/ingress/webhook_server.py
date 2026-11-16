"""FastAPI webhook server — accepts HTTP webhooks and routes them through ETAA.

Guardrails applied before any workflow runs: per-client rate limit (429), body
size cap (413), HMAC signature + timestamp verification (401), and JSON
validation (400). Read endpoints can be protected with an API key.
"""

from __future__ import annotations

import hmac
import json
import os
import threading
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from ..engine.event_router import EventRouter
from ..engine.workflow_executor import WorkflowExecutor
from ..ingress import (
    WebhookIngress,
    resolve_source,
    signing_enabled,
    unsigned_sources,
    verify_signature,
    webhook_secret,
)
from ..observability.logger import setup_logger

log = setup_logger(__name__)

SIGNATURE_HEADER = "X-ETAA-Signature"
TIMESTAMP_HEADER = "X-ETAA-Timestamp"
API_KEY_HEADER = "X-API-Key"
DEFAULT_MAX_BODY_BYTES = 1_048_576  # 1 MiB
DEFAULT_RATE_LIMIT_PER_MINUTE = 600
MAX_RUNS_PAGE = 500


class RateLimiter:
    """Fixed-window request counter per client.

    ponytail: in-memory and per-process, so N workers allow N times the limit.
    Move to a shared store (Redis) or an API gateway when running multi-worker.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._window = 0
        self._counts: dict[str, int] = {}

    def allow(self, key: str, limit: int) -> bool:
        window = int(time.time() // 60)
        with self._lock:
            if window != self._window:
                self._window, self._counts = window, {}
            self._counts[key] = self._counts.get(key, 0) + 1
            return self._counts[key] <= limit


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        log.warning("Ignoring non-integer %s; using %d", name, default)
        return default


async def _read_body(request: Request) -> bytes:
    """Return the raw body, rejecting anything over ``ETAA_MAX_BODY_BYTES`` (413)."""
    max_bytes = _env_int("ETAA_MAX_BODY_BYTES", DEFAULT_MAX_BODY_BYTES)
    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > max_bytes:
        raise HTTPException(status_code=413, detail="Request body too large")
    raw = await request.body()
    if len(raw) > max_bytes:
        raise HTTPException(status_code=413, detail="Request body too large")
    return raw


def _verify(source: str, raw: bytes, request: Request) -> None:
    """Enforce the signing policy for *source*; raise 401 on failure.

    Fails closed: if signing is enabled anywhere and *source* has no usable
    secret (and is not explicitly exempt), the request is rejected.
    """
    secret = webhook_secret(source)
    if secret is not None:
        signature = request.headers.get(SIGNATURE_HEADER)
        timestamp = request.headers.get(TIMESTAMP_HEADER)
        if not verify_signature(secret, raw, signature, timestamp):
            log.warning("Rejected webhook for source=%s: invalid signature", source)
            raise HTTPException(status_code=401, detail="Invalid webhook signature")
    elif signing_enabled() and source not in unsigned_sources():
        log.warning("Rejected webhook for source=%s: signature required", source)
        raise HTTPException(status_code=401, detail="Webhook signature required")


def _parse_body(raw: bytes) -> dict[str, Any]:
    """Parse *raw* as a JSON object, raising 400 on malformed input."""
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=400, detail="Request body is not valid JSON") from exc
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")
    return data


def require_api_key(request: Request) -> None:
    """Protect read endpoints with ``ETAA_API_KEY`` when it is configured."""
    expected = os.environ.get("ETAA_API_KEY")
    if not expected:
        return
    provided = request.headers.get(API_KEY_HEADER, "")
    bearer = request.headers.get("Authorization", "")
    if bearer.startswith("Bearer "):
        provided = provided or bearer[7:]
    if not hmac.compare_digest(provided.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


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
    """Create the FastAPI application around *executor* and *router*."""
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
    limiter = RateLimiter()
    read_auth = [Depends(require_api_key)]

    def rate_limit(request: Request) -> None:
        limit = _env_int("ETAA_RATE_LIMIT_PER_MINUTE", DEFAULT_RATE_LIMIT_PER_MINUTE)
        client = request.client.host if request.client else "unknown"
        if limit > 0 and not limiter.allow(client, limit):
            raise HTTPException(status_code=429, detail="Rate limit exceeded")

    async def dispatch(body: dict[str, Any]) -> JSONResponse:
        event = await wh_ingress.ingest(body)
        runs = await executor.execute(event)
        return JSONResponse(
            content={"event_id": event.event_id, "runs": [r.status.value for r in runs]}
        )

    @app.get(f"{prefix}/health")
    async def health() -> dict[str, Any]:
        return {"status": "ok", "workflows_registered": router.workflow_count}

    @app.post(f"{prefix}/webhook/{{source}}", dependencies=[Depends(rate_limit)])
    async def handle_webhook(source: str, request: Request) -> JSONResponse:
        """Accept a webhook for *source* (from the URL path) and dispatch it."""
        raw = await _read_body(request)
        _verify(source, raw, request)
        body = _parse_body(raw)
        body["source"] = source  # path is authoritative: routed source == verified source
        return await dispatch(body)

    @app.post(f"{prefix}/webhook", dependencies=[Depends(rate_limit)])
    async def handle_any_webhook(request: Request) -> JSONResponse:
        """Generic endpoint for webhooks that carry source/type in the body."""
        raw = await _read_body(request)
        body = _parse_body(raw)
        # Resolve the source exactly as ingest will, then verify against it and
        # pin it, so the verified source is the one that gets routed.
        source = resolve_source(body)
        _verify(source, raw, request)
        body["source"] = source
        return await dispatch(body)

    @app.get(f"{prefix}/workflows", dependencies=read_auth)
    async def list_workflows() -> list[Any]:
        return [wf.model_dump() for wf in router.list_workflows()]

    @app.get(f"{prefix}/runs", dependencies=read_auth)
    async def list_runs(
        workflow_id: str | None = None,
        limit: int = Query(default=50, ge=1, le=MAX_RUNS_PAGE),
    ) -> list[dict]:
        records = executor.history.list(workflow_id=workflow_id, limit=limit)
        return [r.model_dump(mode="json") for r in records]

    @app.get(f"{prefix}/metrics", dependencies=read_auth)
    async def get_metrics() -> dict[str, Any]:
        return executor.metrics.snapshot()

    @app.get(f"{prefix}/metrics/prometheus", dependencies=read_auth)
    async def get_metrics_prometheus() -> PlainTextResponse:
        return PlainTextResponse(executor.metrics.to_prometheus())

    return app
