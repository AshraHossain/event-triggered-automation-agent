---
paths:
  - "tests/**/*.py"
---
# ETAA Testing Rules (`tests/`)
1. All default pytest runs must execute offline using in-memory stores and stubbed outbound HTTP/queue adapters.
2. Cover idempotency deduplication, retry exhaustion -> DLQ routing, and schema validation.
