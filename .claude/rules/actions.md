---
paths:
  - "etaa/actions/**/*.py"
---
# ETAA Action Rules (`etaa/actions/`)
1. Every handler subclasses `BaseAction` and implements
   `async def execute(self, action: ActionSpec, event: EventEnvelope) -> ActionResult`.
2. On failure, raise `ActionError(message, error_code=...)` — the retry/DLQ layer reads
   `error_code` to decide whether to retry. Do not raise bare exceptions.
3. Handlers are config-driven: everything comes from `action.config`. Do not add
   constructor arguments beyond what `BaseAction` provides.
4. Register a new kind in `action_registry` in `etaa/main.py` and add it to the
   `ActionKind` enum in `etaa/models/__init__.py`.
5. `function` actions may only import modules matching `ETAA_FUNCTION_ALLOWLIST`
   (default `examples.,etaa.`). Never widen the default to `*` in committed code.
