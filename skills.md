# ETAA Project Skills

Domain-specific capabilities and patterns for working with the Event-Triggered Automation Agent codebase.

---

## skill: etaa-add-action

**When to use:** Adding a new action type (e.g. S3 upload, gRPC call, GraphQL mutation).

**Steps:**

1. Open `etaa/actions/__init__.py`. Each action is a class inheriting `BaseAction` with an `async def execute(self, action: ActionSpec, event: EventEnvelope) -> ActionResult` method.
2. Return `ActionResult(action_id=..., success=True/False, response=..., error=..., elapsed_seconds=...)`.
3. On failure, raise `ActionError(message, error_code="...", response=...)` — the retry/DLQ layer reads `error_code` to decide whether to retry.
4. Register in `etaa/main.py` inside `action_registry` dict keyed by the string kind (e.g. `"s3": S3Action()`).
5. Add `ActionKind.<NAME> = "name"` to the enum in `etaa/models/__init__.py` if the kind is entirely new.
6. Update `docs/` with usage example.

**Gotcha:** The `config` dict flows directly from the YAML spec into `action.config`. Don't add constructor args beyond what the base class provides — everything is config-driven.

---

## skill: etaa-add-ingress

**When to use:** Supporting a new event source (Kafka, SQS, RabbitMQ, WebSocket).

**Steps:**

1. Subclass `BaseIngress` in `etaa/ingress/__init__.py`.
2. Implement `async def ingest(self, raw) -> EventEnvelope`. Normalise whatever raw shape into the canonical `EventEnvelope`.
3. Add a factory function `make_<name>_ingress()` at the bottom of the file.
4. Wire into `etaa/ingress/webhook_server.py` or create a new CLI entrypoint in `etaa/main.py`.
5. Write a demo script in `examples/<name>/demo.py`.

**Gotcha:** `QueueIngress` already has an in-memory stub with `enqueue()` / `consume_one()`. Use it as a template for pull-based consumers.

---

## skill: etaa-add-workflow

**When to use:** Adding a new workflow definition without touching code.

**Steps:**

1. Create `examples/<name>/workflow.yaml` following the schema in `examples/payment_webhook/workflow.yaml`.
2. Create `examples/<name>/handlers.py` for any `type: function` actions.
3. Create `examples/<name>/demo.py` that calls `build_application(workflows_path=...)` and runs sample events.
4. Run `PYTHONPATH=. python examples/<name>/demo.py` to verify.
5. The benchmark runner (`benchmark.py`) auto-discovers all YAML files under `examples/`.

**Schema reference:** See `etaa/models/__init__.py` → `WorkflowSpec`, `ActionSpec`, `RetryPolicy`, `DLQConfig`.

---

## skill: etaa-debug-run

**When to use:** A workflow run produced unexpected output; need to trace execution.

**Tools:**

```bash
# List recent runs for a workflow
curl http://localhost:8000/api/v1/runs?workflow_id=<wf-id>&limit=10

# Inspect metrics snapshot
curl http://localhost:8000/api/v1/metrics

# Check DLQ file
cat data/dlq/<name>.jsonl | jq '.event, .error'
```

**In code:** The `RunRecord.action_logs` field contains per-attempt detail including `error` string. Access via `get_run_history().list(workflow_id=...)`.

---

## skill: etaa-benchmark

**When to use:** Measuring throughput or comparing configuration changes.

```bash
PYTHONPATH=. python benchmark.py --workflows examples/ --iterations 20
```

Outputs `benchmark_results.json` with per-workflow `total_events`, `success_count`, `failure_count`, `skipped_count`, `avg_latency_ms`.

**Gotcha:** The benchmark replays synthetic events across ALL workflows loaded from `examples/`. If you're benchmarking a single workflow, pass its directory directly: `--workflows examples/payment_webhook/`.

---

## skill: etaa-test

**When to use:** Running the test suite or adding new tests.

```bash
/opt/homebrew/opt/python@3.11/bin/python3.11 -m pytest tests/ -v
```

**Patterns:**
- Model/engine tests go in `tests/test_models_engine.py` (sync, no asyncio needed).
- Action handler tests go in `tests/test_actions.py` (use `@pytest.mark.asyncio`).
- Async tests need `pytest-asyncio>=0.23` installed; the `pyproject.toml` sets `asyncio_mode = "auto"`.

**Idempotency resets:** Each demo script uses singleton stores. For isolated tests, inject a fresh `IdempotencyManager()` instead of using the module-level default.
