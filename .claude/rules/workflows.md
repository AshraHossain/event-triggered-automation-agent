---
paths:
  - "examples/**/*.yaml"
  - "examples/**/*.yml"
---
# ETAA Workflow Spec Rules (`examples/`)
1. YAML keys are snake_case and must validate against the Pydantic models in
   `etaa/models/__init__.py` (`WorkflowSpec`, `WorkflowTrigger`, `ActionSpec`,
   `RetryPolicy`, `DLQConfig`).
2. Trigger matching is OR across `triggers`, AND across `source` / `type` / `filters`
   within a single trigger.
3. `depends_on` must form a DAG; a cycle fails the run (it is no longer silently
   reordered). An action cannot depend on itself.
4. Template placeholders: `{{event_id}}`, `{{source}}`, `{{type}}`,
   `{{payload.<dotted.path>}}`, `{{steps.<action_id>.<path>}}` and
   `{{env.ETAA_SECRET_<NAME>}}`.
5. Never put a token in a spec. Reference it as `{{env.ETAA_SECRET_<NAME>}}`.
6. `ai` actions that feed later steps must set `labels` (and usually `default_label`).
7. DLQ targets: `file` or `db` (SQLite). `queue` only falls back to a file.
