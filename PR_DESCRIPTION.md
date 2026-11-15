# Pull Request Description — Event-Triggered Automation Agent

## Motivation

Event-driven architectures are everywhere: payment webhooks, GitHub push hooks, support ticket creations. Orchestrating reliable responses to these events is harder than it looks. Duplicate deliveries, transient network failures, and silent drops are daily operational problems.

**ETAA** is a open-source Python library that provides a complete, production-ready event orchestration layer:

- **Idempotent execution** — deduplicates events per (workflow, event) pair, preventing double-charges and duplicate notifications.
- **Configurable retry with backoff** — per-action or workflow-level retry policies (fixed or exponential), with selective retryable error codes.
- **Dead-letter queue routing** — exhausted retries are persisted to file, queue, or database for later inspection and replay.
- **Declarative YAML workflows** — define triggers, actions, dependencies, and reliability policies in plain YAML.
- **Structured observability** — JSON logs, in-memory metrics, and persistent run history for debugging and audit.

## Architecture Summary

```
Webhook / Queue Ingress
        │
        ▼
  EventRouter (matches EventEnvelope → WorkflowSpec)
        │
        ▼
  WorkflowExecutor
   ├── IdempotencyManager     (skip duplicates)
   ├── Topological sort       (respect depends_on DAG)
   ├── RetryDLQManager        (retry + backoff + DLQ routing)
   └── Action dispatch        (HTTP / Queue / Function / AI)
        │
        ▼
  Observability                (JSON logger, MetricsStore, RunHistoryStore)
```

### Modules

| Module | Purpose |
|---|---|
| `etaa/models/` | Pydantic v2 schemas: `EventEnvelope`, `WorkflowSpec`, `ActionSpec`, `RetryPolicy`, `DLQConfig`, `RunRecord` |
| `etaa/engine/` | `EventRouter`, `WorkflowExecutor`, `IdempotencyManager`, `RetryDLQManager` |
| `etaa/ingress/` | `WebhookIngress` (FastAPI), `QueueIngress` (in-memory stub) |
| `etaa/actions/` | `HttpAction`, `QueueAction`, `FunctionAction`, `AiAction` |
| `etaa/observability/` | JSON logger, `MetricsStore`, `RunHistoryStore` (JSONL / SQLite) |
| `examples/*/` | Three complete workflow demos with YAML specs and demo scripts |
| `benchmark.py` | Load-test runner producing per-workflow latency & throughput stats |

## How This Improves Reliability, Debuggability, Extensibility

### Reliability
- Idempotency eliminates duplicate-side-effect bugs caused by at-least-once delivery.
- Retry with backoff handles transient failures gracefully instead of failing fast.
- DLQ ensures no event is silently lost — every failure is persisted for review.

### Debuggability
- Every execution produces a `RunRecord` with per-action attempt detail, timestamps, and latency.
- Structured JSON logs make aggregation trivial in any log pipeline.
- Metrics store exposes per-workflow counters (`success_count`, `failure_count`, `dlq_count`, latency percentiles).

### Extensibility
- Adding a new action type: subclass `BaseAction`, implement `execute()`, register in the factory. (~10 lines)
- Adding a new ingress (Kafka, SQS): subclass `BaseIngress`, implement `ingest()`. (~10 lines)
- Swapping backends (Redis idempotency, Prometheus metrics): each is an isolated component with a clean interface.

## Demo Workflows

1. **Payment Webhook** — Stripe `payment_succeeded` → HTTP email notification → CRM function update. Exponential retry on email; file-based DLQ.
2. **GitOps Automation** — GitHub `push` to `main` → CI trigger function → Slack notification HTTP. Fixed backoff retry; queue-based DLQ.
3. **Support Ticket Flow** — `ticket_created` → AI classification (stub) → owner assignment function → ack email HTTP. Full DAG with `depends_on`.

## Getting Started

```bash
pip install -e ".[dev]"
PYTHONPATH=. python examples/payment_webhook/demo.py
PYTHONPATH=. python benchmark.py --iterations 10
```

Server mode:
```bash
python -m etaa.main server --workflows examples/ --port 8000
curl -X POST http://localhost:8000/api/v1/webhook/stripe \
  -d '{"id":"e1","source":"stripe","type":"payment_succeeded","data":{...}}'
```

## Open Questions for Reviewers

- Should the default idempotency TTL be shorter (5 min) or longer (24 h)?
- Is the YAML-only workflow spec sufficient, or should we add JSON Schema validation?
- Priority for next: real Kafka/SQS ingress adapters, or a REST admin API for workflow management?

## Invite to Contribute

This is a foundation, not a finished product. We invite the community to:

- **Review the abstractions** — especially the separation between engine, ingress, and actions.
- **Propose new action types** — GraphQL, gRPC, S3 upload, cloud function invocation.
- **Implement real queue adapters** — AWS SQS, RabbitMQ, Apache Kafka.
- **Add a replay CLI** — `etaa replay --dlq ./data/dlq/payment.jsonl --workflow payment-webhook`.
- **Improve observability** — Prometheus metrics export, OpenTelemetry tracing.

Reliable event automation shouldn't require building from scratch. Let's build it together.
