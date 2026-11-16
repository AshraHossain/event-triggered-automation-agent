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
4. Template placeholders in action config: `{{event_id}}`, `{{source}}`, `{{type}}`, and
   `{{payload.<dotted.path>}}`.
5. Keep secrets out of specs — reference credentials via headers/env at runtime, never
   inline.
