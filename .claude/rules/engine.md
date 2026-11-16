---
paths:
  - "etaa/**/*.py"
---
# ETAA Engine Rules (`etaa/`)
1. Enforce idempotency key checks prior to action execution.
2. Keep retry/backoff calculation and DLQ routing deterministic and side-effect-free.
3. Never hardcode webhook secrets; validate signatures at the ingestion boundary.
