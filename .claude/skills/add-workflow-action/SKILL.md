---
name: add-workflow-action
description: Use this skill when adding a new declarative workflow action type or webhook/queue ingress adapter in ETAA.
---
# Add a Workflow Action (or Ingress) in ETAA

## New action type
1. Write failing offline tests in `tests/` first: happy path, a duplicate `event_id`
   (idempotency → SKIPPED), and a failure that exhausts retries → DLQ. Use the
   `make_executor` fixture.
2. Add `etaa/actions/<kind>.py` with a class subclassing `BaseAction`:
   `async def execute(self, action, event, steps=None) -> ActionResult`. Read everything
   from `action.config`, render placeholders with `etaa.templating.render`, and raise
   `ActionError(msg, error_code=..., retryable=...)` on failure.
3. Export it from `etaa/actions/__init__.py` and add the kind to `ActionKind` in
   `etaa/models/__init__.py`.
4. Register it in `action_registry` inside `build_application` (`etaa/main.py`).
5. Document it in the README "Workflows" section and add an example if it is user-facing.
6. `make check`.

## New ingress adapter
1. Subclass `BaseIngress` in `etaa/ingress/__init__.py`; implement
   `async def ingest(self, raw) -> EventEnvelope`, reusing `WebhookIngress`
   normalisation where possible. Use `resolve_source` so routing and signature
   verification agree.
2. Add a factory helper and wire it into `etaa/main.py` or a new server module.
3. Add an offline test (no network).
4. `make check`.

Keep everything offline and deterministic; the only external call is the optional
`ai` action, which stubs out without `OPENAI_API_KEY`.
