# AGENTS.md — Event-Triggered Automation Agent (ETAA)

Portable instructions for **Claude Code**, **Antigravity (Gemini)**, and other coding agents.

## What this project is
A vendor-agnostic event orchestration engine (`etaa/`). It normalises webhooks and
queue messages into a canonical `EventEnvelope`, matches them against declarative YAML
`WorkflowSpec`s, and executes actions with idempotency, configurable retry/backoff, and
dead-letter-queue (DLQ) routing. Python 3.11+, Pydantic v2, FastAPI, async IO.

## Setup & commands
```bash
uv venv && uv pip install -e ".[dev]"   # create .venv and install (do this first)
uv run pytest -q                         # offline unit + integration suite
uv run pytest -q --cov=etaa              # with coverage (CI gate: >=80%)
uv run ruff check . && uv run black --check .   # lint + format
uv run python benchmark.py               # throughput/latency -> benchmark_results.json
PYTHONPATH=. uv run python examples/payment_webhook/demo.py   # a demo
uv run python -m etaa.main server --workflows examples/ --port 8000   # HTTP server
```
There is no system-wide `python3.11`; always go through `uv`/`.venv`, never a hardcoded
interpreter path.

## Layout
- `etaa/models/` — Pydantic schemas (`EventEnvelope`, `WorkflowSpec`, `ActionSpec`, `RetryPolicy`, `DLQConfig`, `RunRecord`).
- `etaa/engine/` — `EventRouter`, `WorkflowExecutor`, `IdempotencyManager`, `RetryDLQManager`.
- `etaa/ingress/` — webhook/queue normalisation + the FastAPI server (`webhook_server.py`).
- `etaa/actions/` — action handlers: HTTP, queue, local function, AI.
- `etaa/observability/` — JSON logger, metrics store, run-history store (JSONL or SQLite).
- `examples/` — three end-to-end workflow demos (YAML + handlers + demo script).
- `tests/` — offline suite (respx-mocked HTTP, in-memory stores).

## Repo-specific rules
- **Idempotency first**: `WorkflowExecutor` calls `IdempotencyManager.check_and_set`
  before running any action; duplicates become a `SKIPPED` run with no side effects.
- **Deterministic core**: retry/backoff math, DLQ routing, and topological ordering live
  in `etaa/` Python — never inside an LLM prompt. The only LLM call is the optional
  `ai` action, which falls back to a deterministic stub without `OPENAI_API_KEY`.
- **Offline tests**: everything under `tests/` must run with no network — mock HTTP with
  respx and use in-memory stores.

## Security configuration (env vars)
- `ETAA_WEBHOOK_SECRET` / `ETAA_WEBHOOK_SECRET_<SOURCE>` — enable HMAC-SHA256 signature
  verification on inbound webhooks (`X-ETAA-Signature: sha256=<hex>`). When any secret is
  set, unconfigured sources **fail closed** (401) unless listed in
  `ETAA_WEBHOOK_UNSIGNED_SOURCES`.
- `ETAA_FUNCTION_ALLOWLIST` — comma-separated module prefixes the `function` action may
  import (default `examples.,etaa.`; `*` allows any — not recommended).

## Gotchas
- **Singleton stores**: `get_metrics_store()`, `get_run_history()`,
  `get_idempotency_manager()` return module-level singletons; state carries across runs in
  one process. Inject fresh instances in tests; call `.close()` between experiments.
- **PYTHONPATH**: demo scripts need the project root importable (`PYTHONPATH=.` or an
  editable install).
- **Recursive workflow loading**: `load_workflows(dir)` scans recursively and dedupes by
  path; any stray `*.yaml` under the directory is registered.
- **Demos hit the network**: example workflows POST to placeholder URLs, so demo/benchmark
  runs report `failed`/`dlq` by design — they exercise the retry + DLQ paths, not success.

See `.claude/rules/` (linked from `.agents/rules/`) for path-scoped rules and
`.claude/skills/` (linked from `.agents/skills/`) for repo-specific procedures.
