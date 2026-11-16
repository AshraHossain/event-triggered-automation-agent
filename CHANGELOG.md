# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
