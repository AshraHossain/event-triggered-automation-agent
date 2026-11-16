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
6. Verify (see below).

## New ingress adapter
1. Subclass `BaseIngress` in `etaa/ingress/__init__.py`; implement
   `async def ingest(self, raw) -> EventEnvelope`, reusing `WebhookIngress`
   normalisation where possible. Use `resolve_source` so routing and signature
   verification agree.
2. Add a factory helper and wire it into `etaa/main.py` or a new server module.
3. Add an offline test (no network).
4. Verify (see below).

## Verify
Use the allowlisted exact forms so the checks run without permission prompts:

1. While iterating: `ruff check etaa -q` (lint only, read-only).
2. To apply fixes: `ruff check . --fix` then `black .` — these rewrite files and will
   prompt; never append `--fix` to the allowlisted lint commands.
3. Before committing: `make check` (lint, format, types, security scan, tests with the
   coverage gate). Run it alone, not chained with other commands.
4. Before merging or reporting the work as done, run the `enterprise-release-gates` skill
   and report its gate table (unrun checks are `not verified`).
5. Commit with `git commit` as its own command (no `grep -n` or other `-n` flag in the same
   call — the no-verify hook blocks it).

Keep everything offline and deterministic; the only external call is the optional
`ai` action with `provider: anthropic` or `openai`. Tests use `provider: stub` or inject
a fake provider module.
