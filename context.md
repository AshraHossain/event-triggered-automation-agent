# ETAA — Project Context

## What is this?

**Event-Triggered Automation Agent (ETAA)** is a Python 3.11+ library that provides a vendor-agnostic event orchestration layer. It normalises incoming webhooks and queue messages into a canonical `EventEnvelope`, matches them against declarative YAML workflow specs, and executes actions with idempotency, configurable retry with backoff, and dead-letter queue routing.

**Status:** Beta — production-ready architecture, some adapters still stubbed (SQS, Kafka, real AI providers).

---

## Architecture

```
Raw event (HTTP / queue)
        │
        ▼
  Ingress (WebhookIngress / QueueIngress)
        │  normalises to
        ▼
  EventEnvelope { event_id, source, type, payload, received_at }
        │
        ▼
  EventRouter — matches against WorkflowSpec.triggers
        │
        ▼
  WorkflowExecutor
   ├── IdempotencyManager.check_and_set(wf_id, event_id)
   │     True  → status=SKIPPED (already processed)
   │     False → continue
   │
   ├── Topological sort (Kahn's algorithm on depends_on)
   │
   └── For each action:
         RetryDLQManager.run_with_retry()
           ├─ execute via ActionFactory (http/queue/function/ai)
           ├─ catch ActionError
           ├─ retry with backoff if should_retry(error_code, attempt)
           └─ exhausted → DLQ routing (file/queue/db)
        │
        ▼
  RunHistoryStore (JSONL or SQLite) + MetricsStore (in-memory + SQLite)
```

### Module map

| Path | Responsibility | Key classes/functions |
|---|---|---|
| `etaa/models/__init__.py` | All data schemas | `EventEnvelope`, `WorkflowSpec`, `ActionSpec`, `RetryPolicy`, `DLQConfig`, `RunRecord` |
| `etaa/engine/event_router.py` | Trigger matching | `EventRouter` |
| `etaa/engine/idempotency_manager.py` | Deduplication | `IdempotencyManager` |
| `etaa/engine/retry_dlq_manager.py` | Retry + DLQ | `RetryDLQManager` |
| `etaa/engine/workflow_executor.py` | Main orchestration loop | `WorkflowExecutor` |
| `etaa/ingress/__init__.py` | Raw → EventEnvelope | `WebhookIngress`, `QueueIngress` |
| `etaa/ingress/webhook_server.py` | FastAPI HTTP server | `create_app()` |
| `etaa/actions/__init__.py` | Action dispatch | `HttpAction`, `QueueAction`, `FunctionAction`, `AiAction` |
| `etaa/observability/logger.py` | Structured logs | `setup_logger()` |
| `etaa/observability/metrics_store.py` | Counters + latency | `MetricsStore` |
| `etaa/observability/run_history_store.py` | Persistent runs | `RunHistoryStore` |
| `etaa/main.py` | App factory + CLI | `build_application()`, `main()` |

---

## Conventions

### Code style
- Python 3.11+ with full type hints.
- Pydantic v2 for all models (`BaseModel`).
- Async/await for all IO paths (httpx, queue ops).
- Sync fallback (`time.sleep`) in retry manager for non-async callers — keep signatures compatible.
- Docstrings on every public class and method; include a one-line usage snippet.

### Naming
- Models: PascalCase (`EventEnvelope`, `WorkflowSpec`).
- Engine classes: PascalCase (`WorkflowExecutor`, `IdempotencyManager`).
- Action handlers: PascalCase inheriting `BaseAction` (`HttpAction`, `FunctionAction`).
- Private helpers: snake_case with leading underscore (`_topological_sort`, `_load_single`).
- YAML keys: kebab-case (`backoff_strategy`, `retryable_error_codes`).

### Error handling
- Always raise `ActionError` (not bare `Exception`) from action handlers. Carries `error_code` for retry logic.
- Retryable codes are strings or ints — comparison is done via `str(error_code)`.
- Non-retryable errors (connection refused, 404) skip the retry loop immediately.

### Defaults
- Retry: `max_attempts=3`, `exponential`, `backoff_seconds=1.0`.
- Idempotency TTL: 3600s (1 hour).
- DLQ target type: `file`.
- Metrics persist to SQLite only when `persist_path` is explicitly set.

---

## How to Extend

### New action type
1. Add class to `etaa/actions/__init__.py`.
2. Add kind to `ActionKind` enum in `etaa/models/__init__.py`.
3. Register in `action_registry` dict in `etaa/main.py` line ~79.

### New ingress type
1. Subclass `BaseIngress` in `etaa/ingress/__init__.py`.
2. Add factory function.
3. Wire into app in `etaa/ingress/webhook_server.py` or create a new server module.

### Change idempotency backend
Replace `IdempotencyManager` with a Redis-backed class implementing `check_and_set(wf_id, event_id) -> bool`. Pass it into `WorkflowExecutor.__init__`. The interface is two methods: `check_and_set` and `clear`.

### Change DLQ backend
Add a new `target_type` branch in `RetryDLQManager._send_to_dlq()`. The record shape is fixed — see the JSONL output format in `data/dlq/*.jsonl`.

---

## Gotchas

1. **Singleton stores:** `get_metrics_store()`, `get_run_history()`, `get_idempotency_manager()` return module-level singletons. Consecutive demo runs accumulate state. Call `.close()` on the store or restart the process between experiments.
2. **Asyncio mode:** Tests require `pytest-asyncio>=0.23`. Older versions don't support `asyncio_mode = "auto"`. The pyproject.toml pins the right version.
3. **YAML loading is recursive:** `load_workflows("examples/")` scans top-level AND subdirectories. A YAML file named incorrectly anywhere under `examples/` will be loaded and may register unexpected workflows.
4. **HTTP action templates:** `{{event_id}}`, `{{source}}`, `{{type}}` are the only built-in placeholders. Custom template variables require modifying `HttpAction._template()`.
5. **Function action imports:** Uses `importlib.import_module()` at runtime. The module must be importable from the current `sys.path`. Set `PYTHONPATH=.` when running demos.
6. **Topological sort fallback:** If a cycle is detected in `depends_on`, the executor falls back to original order with a warning — it does NOT error out. Verify your DAG is acyclic before shipping.
7. **Python path:** The project root must be on `sys.path` (via `PYTHONPATH=.` or `pip install -e .`). Running scripts from subdirectories without this fails with `ModuleNotFoundError: No module named 'etaa'`.

---

## Verified Commands

```bash
# Install
/opt/homebrew/opt/python@3.11/bin/python3.11 -m pip install -e ".[dev]"

# Run tests
/opt/homebrew/opt/python@3.11/bin/python3.11 -m pytest tests/ -v

# Demo: payment webhook
PYTHONPATH=. /opt/homebrew/opt/python@3.11/bin/python3.11 examples/payment_webhook/demo.py

# Demo: gitops automation
PYTHONPATH=. /opt/homebrew/opt/python@3.11/bin/python3.11 examples/gitops_automation/demo.py

# Demo: support ticket flow
PYTHONPATH=. /opt/homebrew/opt/python@3.11/bin/python3.11 examples/support_ticket_flow/demo.py

# Benchmark
PYTHONPATH=. /opt/homebrew/opt/python@3.11/bin/python3.11 benchmark.py --iterations 10

# Start webhook server
PYTHONPATH=. /opt/homebrew/opt/python@3.11/bin/python3.11 -m etaa.main server --workflows examples/ --port 8000

# Send a test webhook
curl -X POST http://localhost:8000/api/v1/webhook/stripe \
  -H "Content-Type: application/json" \
  -d '{"id":"evt-x","source":"stripe","type":"payment_succeeded","data":{"amount":100,"success":true}}'

# Check metrics
curl http://localhost:8000/api/v1/metrics
curl http://localhost:8000/api/v1/runs
```

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| Pydantic v2 models | Validation + serialization in one; works with FastAPI natively |
| In-memory idempotency by default | Simpler for demo/single-process; easy to swap for Redis |
| JSONL for run history (default) | Human-readable, append-only, no DB dependency for basic use |
| `ActionError` with `error_code` | Gives retry logic granularity without coupling to specific exception types |
| Topological sort for `depends_on` | Supports arbitrary DAGs, not just linear chains |
| YAML-only workflow specs | Declarative, version-control friendly, no Python import needed for config |
| Stub AI action returns hash | Deterministic output for testing; real provider integration is TODO |
