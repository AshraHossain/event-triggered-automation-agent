# AGENTS.md — Event-Triggered Automation Agent (ETAA)

Portable instructions for **Antigravity (Gemini)**, **Claude Code**, and other coding agents.

## What this project is
A vendor-agnostic event orchestration engine (`etaa/`) that ingests webhooks and queue events, executes declarative YAML workflows with idempotency, configurable retry/backoff, and dead-letter queue (DLQ) routing.

## Commands
```bash
uv run pytest -q                                     # run offline unit + integration suite
uv run python benchmark.py                           # run throughput/latency benchmark -> benchmark_results.json
uv run ruff check .                                  # lint check
```

## Layout
- `etaa/` — Core engine, webhook/queue listeners, workflow runner, idempotency store, retry/backoff, and DLQ router.
- `examples/` — Declarative YAML workflow definitions and sample event payloads.
- `tests/` — Offline test suite for idempotency, retries, DLQ routing, and actions.
- `benchmark.py` — Deterministic performance benchmark script.

## Repo-Specific Rules
- **Idempotency First**: Every event handler must check and record idempotency keys before executing outbound side effects.
- **Deterministic Core**: Retry backoff math, circuit breaking, and DLQ routing live in `etaa/` Python modules — never inside LLM prompts.
- **Offline Tests**: All tests in `tests/` must run offline without external webhook/queue network dependencies.
