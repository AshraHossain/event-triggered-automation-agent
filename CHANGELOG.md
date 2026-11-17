# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.2.0] - 2026-11-16

### Added
- Webhooks are acknowledged with `202` and processed in the background; `?wait=true`
  keeps the synchronous behaviour. `ETAA_MAX_IN_FLIGHT` bounds background work (`503`).
- Durable inbox: accepted events are recorded before the acknowledgement and resumed on
  the next start.
- Step checkpoints: a redelivery or dead-letter replay resumes at the failed action
  instead of repeating completed ones.
- `etaa validate` and load-time validation of workflow graphs (duplicate ids, unknown
  dependencies, cycles).
- Classifier evaluation (`make eval`): 32 labelled tickets, accuracy gate, scorecard.

### Changed
- `ETAA_IDEMPOTENCY_DB` is replaced by `ETAA_STATE_DB`, one SQLite file for idempotency
  claims, checkpoints and the inbox.
- The default webhook response is `202 {"status": "accepted"}` instead of `200` with run
  statuses.

## [0.1.0] - 2026-11-16

First tagged release.

### Added
- Event ingestion (webhooks, in-memory queue) normalised to `EventEnvelope`.
- Declarative YAML workflows with trigger filters and a `depends_on` DAG.
- Actions: `http`, `queue`, `function`, `ai` (Anthropic, OpenAI, offline stub).
- Step outputs: `{{steps.<action_id>.<path>}}` and `context["steps"]`.
- Idempotency with in-memory or SQLite storage; claims are released on failure.
- Retry with fixed/exponential backoff; non-retryable errors skip the retry budget.
- Dead-letter stores (JSONL, SQLite) and `etaa replay`.
- `--dry-run` for `run`, `replay` and `server`.
- Run history (JSONL or SQLite), JSON metrics and a Prometheus endpoint.
- Security: HMAC webhook signatures with timestamp replay protection (fail closed),
  body-size cap, rate limit, API key on read endpoints, outbound SSRF guard,
  function-import allowlist, `{{env.ETAA_SECRET_*}}` secret references, and
  label-constrained AI output.
- Offline demos, a benchmark, Dockerfile, Makefile, and CI (ruff, black, mypy,
  bandit, pip-audit, pytest with a 90% coverage gate).
