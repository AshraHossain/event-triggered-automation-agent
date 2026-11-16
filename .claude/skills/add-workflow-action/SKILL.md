---
name: add-workflow-action
description: Use this skill when adding a new declarative workflow action type or webhook/queue ingress adapter in ETAA.
---
# Add a Workflow Action (or Ingress) in ETAA

## New action type
1. Write a failing offline test in `tests/` first: happy path, a duplicate
   `event_id` (idempotency → SKIPPED), and a failure that exhausts retries → DLQ.
2. Add a handler class in `etaa/actions/__init__.py` subclassing `BaseAction` with
   `async def execute(self, action, event) -> ActionResult`. Read everything from
   `action.config`; raise `ActionError(msg, error_code=...)` on failure.
3. Add the kind to `ActionKind` in `etaa/models/__init__.py`.
4. Register it in `action_registry` in `etaa/main.py`.
5. `uv run pytest -q` and `uv run ruff check . && uv run black --check .`.

## New ingress adapter
1. Subclass `BaseIngress` in `etaa/ingress/__init__.py`; implement
   `async def ingest(self, raw) -> EventEnvelope`, reusing `WebhookIngress`
   normalisation where possible. Use `resolve_source` so routing and signature
   verification agree.
2. Add a factory helper and wire it into `etaa/main.py` or a new server module.
3. Add an offline test (no network).

Keep everything offline and deterministic; the only external call is the optional
`ai` action, which stubs out without `OPENAI_API_KEY`.
