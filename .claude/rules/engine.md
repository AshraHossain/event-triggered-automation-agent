---
paths:
  - "etaa/engine/**/*.py"
  - "etaa/ingress/**/*.py"
---
# ETAA Engine & Ingress Rules
1. Claim the idempotency key before any action runs; release it when the run fails.
2. Keep retry/backoff calculation and DLQ routing deterministic. Delays go through the
   injected `sleep` in `RetryDLQManager`, never a direct `time.sleep`.
3. A failed event must be persisted by `dlq.write` or the failure must be logged at ERROR
   with `dlq_routed=False`. Never add a DLQ target that only logs.
4. SQL identifiers (table names) are validated by `dlq._table`; values are always bound
   parameters.
5. Never hardcode webhook secrets. Verify signatures on the raw body before parsing, and use
   `resolve_source` so the verified source is the routed source.
6. New request guardrails read their limits from `ETAA_*` env vars with a safe default.
