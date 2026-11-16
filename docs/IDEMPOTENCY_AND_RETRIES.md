# Idempotency & Retries in Event-Driven Systems

## Why It Matters

Event-driven architectures trade consistency for resilience. Messages are delivered **at least once** — the network will retry, queues will redeliver, and failures happen. Without idempotency guarantees, every downstream operation (charge a card, send an email, update a database row) risks executing twice, producing incorrect state.

**Exactly-once semantics** are theoretically possible but practically expensive (two-phase commit, transactional outboxes). ETAA targets **idempotent-at-least-once**: the system may deliver an event multiple times, but the *observable outcome* is identical to a single delivery.

## Patterns

### 1. Idempotency Keys

The most common pattern: derive a deterministic key from the event and check whether it has already been processed.

```
idempotency_key = f"{workflow_id}:{event_id}"
```

If the key exists in the store, skip execution. This works because:
- `event_id` is unique per event (issued by the producer).
- `workflow_id` scopes deduplication per workflow (the same event might legitimately trigger different workflows).

ETAA implements this in `IdempotencyManager` (default TTL 1 hour). The key is claimed before the run starts, so a concurrent duplicate is skipped, and released if the run fails, so a redelivery or a DLQ replay is processed rather than skipped.

The default store is in-memory. Set `ETAA_IDEMPOTENCY_DB=/path/idempotency.db` for a SQLite store that survives restarts; the claim is an `INSERT OR IGNORE` on a primary key, so it is atomic across processes on one host. For several hosts, back the same interface with Redis `SET NX EX` or a database UNIQUE constraint.

Because a failed run releases its key, a retry re-runs the workflow from its first action: actions should be safe to repeat (at-least-once per action).

### 2. Retry with Backoff

Transient failures (network timeouts, 503 Service Unavailable, rate limits) should not cause immediate workflow failure. Instead:

1. Catch the error.
2. Skip the retry if the error can never succeed (`ActionError(retryable=False)`: bad config, forbidden URL, missing API key). Otherwise check the error code against `retryable_error_codes` (empty list = retry everything).
3. Wait `backoff_delay` seconds.
4. Retry up to `max_attempts`.
5. On exhaustion, route to DLQ.

Backoff strategies:
- **Fixed**: constant delay (simple, good for rate-limit backoff).
- **Exponential**: delay doubles each attempt, capped at 60s (better for overloaded services).

```python
policy = RetryPolicy(
    max_attempts=3,
    backoff_strategy="exponential",
    backoff_seconds=1.0,
    retryable_error_codes=[429, 500, 502, 503, "timeout"],
)
delay = policy.compute_delay(attempt=2)  # 2.0 seconds
```

### 3. Dead-Letter Queues (DLQ)

When all retries are exhausted, the event must go somewhere — not disappear. The DLQ is a holding area for failed events that you can:

- Inspect later to understand why they failed.
- Fix the root cause and replay them.
- Manually escalate to a human.

ETAA supports three DLQ targets:
- **File** (`target_type: "file"`): append to a JSONL file. Best for local development and debugging.
- **DB** (`target_type: "db"`): insert into a SQLite table (`db_path`, `table_name`). Best when you need SQL queries over failed events.
- **Queue** (`target_type: "queue"`): no broker adapter exists yet, so the event is written to `./data/dlq/<queue_name>.jsonl` with a warning. It is never dropped.

Replay a store with `etaa replay <path> --workflows <dir>`. Processed records are removed; an event that fails again is dead-lettered again.

DLQ records include the original payload and the last error (configurable via `include_payload` / `include_error`).

## How ETAA Implements It

```
Event arrives
     │
     ▼
IdempotencyManager.check_and_set(wf_id, event_id)
     │ True  → skip (return SKIPPED)
     │ False → continue
     ▼
Topological sort of actions (respect depends_on)
     │
     ▼
For each action:
  RetryDLQManager.run_with_retry(action, event, executor, policy, dlq)
      │
      ├─ Attempt 1..N with backoff
      │    ├─ Success → return ActionResult(success=True)
      │    └─ Failure → check should_retry(error_code, attempt)
      │         ├─ Yes  → sleep(delay), retry
      │         └─ No   → fall through to DLQ
      │
      └─ All attempts exhausted → DLQ routing
           ├─ File: append JSONL
           ├─ DB: insert row (SQLite)
           └─ Queue: fall back to a JSONL file
           then release the idempotency key
```

Run history (`RunHistoryStore`) persists every execution — status, attempts, latency, action-level detail — enabling post-mortems and replay.

## Checklist for Production Use

- [x] Persistent idempotency store (`ETAA_IDEMPOTENCY_DB`, SQLite)
- [x] DLQ replay tooling (`etaa replay`)
- [x] Prometheus metrics endpoint (`/api/v1/metrics/prometheus`)
- [ ] Shared idempotency store for multi-host deployments (Redis / database)
- [ ] Real SQS / RabbitMQ / Kafka queue adapters
- [ ] Alerting on the `dlq_count` metric
- [ ] Appropriate TTLs (idempotency window vs. DLQ retention)
