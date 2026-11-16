---
name: add-workflow-action
description: Use this skill when adding a new declarative workflow action type or webhook adapter in ETAA.
---
# Add a Workflow Action in ETAA
1. Define immutable config/payload validation in `etaa/`.
2. Write failing offline tests in `tests/` covering happy path, duplicate idempotency key, and retry -> DLQ failure.
3. Implement the action handler behind the adapter interface.
4. Run `uv run pytest -q` and `uv run python benchmark.py`.
