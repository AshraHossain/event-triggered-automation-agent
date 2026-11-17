# AGENTS.md — Event-Triggered Automation Agent (ETAA)

Portable instructions for **Claude Code**, **Antigravity (Gemini)**, and other coding agents.

## What this project is
A vendor-agnostic event orchestration engine (`etaa/`). It normalises webhooks and queue
messages into an `EventEnvelope`, matches them against declarative YAML `WorkflowSpec`s, and
runs actions with idempotency, retry/backoff, and a replayable dead-letter queue.
Python 3.11+, Pydantic v2, FastAPI, async IO.

## Setup & commands
```bash
make install                 # uv venv && uv pip install -e ".[dev]"  (do this first)
make check                   # ruff, black, mypy, bandit, pip-audit, pytest (>= 90%), validate, eval
make eval                    # classifier accuracy on evals/tickets.jsonl (offline)
make test                    # tests only
make demo                    # offline payment demo
make benchmark               # writes benchmark_results.json
uv run etaa server --workflows examples/ --port 8000 --dry-run
uv run etaa replay data/dlq/payment.jsonl --workflows examples/payment_webhook
uv run etaa validate examples/
uv run etaa consume <queue> --db ./data/queues.db --workflows examples/ --once
```
There is no system `python3.11`; always go through `uv` / `.venv`, never a hardcoded
interpreter path.

## Layout
- `etaa/models/` — Pydantic schemas (`EventEnvelope`, `WorkflowSpec`, `ActionSpec`, `RetryPolicy`, `DLQConfig`, `RunRecord`).
- `etaa/engine/` — `EventRouter`, `WorkflowExecutor`, `IdempotencyManager`, `RetryDLQManager`, `dlq.py` (dead-letter store), `state_store.py` (step checkpoints, inbox).
- `etaa/ingress/` — normalisation and signature helpers (`__init__.py`), FastAPI server (`webhook_server.py`).
- `etaa/actions/` — one module per action type (`http.py`, `queue.py`, `function.py`, `ai.py`), shared types in `base.py`.
- `etaa/templating.py` — placeholder rendering used by every action.
- `etaa/observability/` — JSON logger, metrics store, run-history store.
- `etaa/queues.py` — durable SQLite queue (`publish`/`claim`/`ack`, redelivery, poison messages).
- `etaa/main.py` — `build_application`, workflow loading, CLI (`server`, `run`, `replay`, `consume`, `validate`, `list`, `metrics`).
- `evals/` — labelled tickets and `run_eval.py` for the classifier.
- `examples/` — three workflows; demos share `examples/demo_support.py` (mock HTTP transport).
- `tests/` — offline suite; `conftest.py` provides `make_executor` and `app_env`.

## Repo-specific rules
- **Idempotency first**: `WorkflowExecutor.run_workflow` claims `(workflow_id, event_id)`
  before any action and releases it when the run fails. Keep both halves.
- **Resume, do not repeat**: each successful action is checkpointed; a retry skips them
  (`status="resumed"`). Checkpoints are cleared when the run succeeds.
- **Acknowledge after recording**: the server puts the event in the `Inbox` before
  returning `202` and removes it when processing finishes. Never do slow work before the ack.
- **Never drop a failed event**: every failure path ends in `dlq.write`. A new DLQ target
  must persist or raise; logging alone is not acceptable.
- **Deterministic core**: retry/backoff maths, DLQ routing, ordering and label validation
  live in Python, never in a prompt. The `ai` action only proposes a value, which the code
  then constrains to `labels`.
- **Offline tests**: nothing under `tests/` may use the network. Use
  `httpx.MockTransport` (via `make_executor(transport)`) or respx, and inject fake
  `openai` / `anthropic` modules for provider tests.
- **Errors**: raise `ActionError(message, error_code=..., retryable=...)`. Use
  `retryable=False` for anything a retry cannot fix (bad config, forbidden target).

## Security configuration (env vars)
| Variable | Effect |
|---|---|
| `ETAA_WEBHOOK_SECRET`, `ETAA_WEBHOOK_SECRET_<SOURCE>` | HMAC-SHA256 over `timestamp.body` (`X-ETAA-Signature`, `X-ETAA-Timestamp`); fail closed once any secret is set |
| `ETAA_WEBHOOK_UNSIGNED_SOURCES` | Sources exempt from signing |
| `ETAA_WEBHOOK_TOLERANCE_SECONDS` | Replay window (default 300) |
| `ETAA_MAX_BODY_BYTES`, `ETAA_RATE_LIMIT_PER_MINUTE` | Request limits (1 MiB, 600) |
| `ETAA_API_KEY` | Protects `/runs`, `/metrics`, `/workflows` |
| `ETAA_HTTP_ALLOWED_HOSTS`, `ETAA_HTTP_ALLOW_PRIVATE` | Outbound HTTP guard |
| `ETAA_FUNCTION_ALLOWLIST` | Module prefixes `function` actions may import (default `examples.,etaa.`) |
| `ETAA_SECRET_*` | The only env vars a workflow can read, via `{{env.ETAA_SECRET_X}}` |
| `ETAA_STATE_DB` | SQLite file for idempotency claims, step checkpoints and the inbox |
| `ETAA_MAX_IN_FLIGHT` | Cap on events being processed in the background (default 1000, then 503) |

## Gotchas
- **Singleton stores**: `get_metrics_store()`, `get_run_history()` and
  `get_idempotency_manager()` are process-wide and used by `build_application`. Tests should
  build an executor with `make_executor` instead.
- **Runtime data**: demos, the CLI and the benchmark write under `./data/` (gitignored).
- **Recursive workflow loading**: `load_workflows(dir)` registers every `*.yaml` under the
  directory.
- **Webhook tests**: the default response is `202`; use `?wait=true` when a test needs the
  run statuses in the response.
- **Classifier rules are not tuned to `evals/tickets.jsonl`**; do not edit `stub_rules` to
  raise the score.
- **Hooks**: a `git commit` command that also contains `-n` anywhere (for example
  `grep -n`) is blocked by the no-verify hook. Run the commit on its own.

See `.claude/rules/` (linked from `.agents/rules/`) for path-scoped rules and
`.claude/skills/` (linked from `.agents/skills/`) for repo-specific procedures.
