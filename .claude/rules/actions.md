---
paths:
  - "etaa/actions/**/*.py"
  - "etaa/templating.py"
---
# ETAA Action Rules (`etaa/actions/`)
1. One module per action type. Every handler subclasses `BaseAction` and implements
   `async def execute(self, action, event, steps=None) -> ActionResult`.
2. On failure raise `ActionError(message, error_code=..., retryable=...)`. Set
   `retryable=False` when a retry cannot help (bad config, forbidden target, missing key).
3. Handlers are config-driven: read everything from `action.config`. Constructor arguments
   are only for test seams (for example `HttpAction(transport=...)`).
4. Render placeholders with `etaa.templating.render` / `render_value`; do not write another
   template function. Use `url_safe=True` for anything that becomes part of a URL.
5. Outbound URLs must pass `validate_url`. Do not weaken the private-host block or the
   `ETAA_SECRET_` prefix check.
6. Register a new kind in `build_application` (`etaa/main.py`), add it to `ActionKind`, and
   export it from `etaa/actions/__init__.py`.
7. The `ai` action's result must pass through `_constrain` when `labels` is set.
