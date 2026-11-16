# Event-Triggered Automation Agent (ETAA)

A production-grade, vendor-agnostic event orchestration layer that listens to webhooks and message queues, executes declarative workflows with guaranteed idempotency, configurable retry with backoff, and dead-letter queue routing.

```
┌─────────────┐     ┌──────────────┐     ┌──────────────────┐
│  Webhooks   │────▶│              │────▶│                  │
│  (Stripe,   │     │   ETAA       │     │  Workflow        │
│   GitHub,   │◀────│  Engine      │◀────│  Specs (YAML)    │
│   Custom)   │     │              │     │                  │
└─────────────┘     └──────────────┘     └──────────────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │  Actions             │
              │  • HTTP  (outbound)  │
              │  • Queue (pub/sub)   │
              │  • Function (local)  │
              │  • AI      (stub)    │
              └──────────────────────┘
                         │
              ┌──────────▼──────────┐
              │  Reliability Layer  │
              │  • Idempotency      │
              │  • Retry + Backoff  │
              │  • Dead-Letter Q    │
              └──────────┬──────────┘
                         │
              ┌──────────▼──────────┐
              │  Observability      │
              │  • JSON Logs        │
              │  • Metrics (in-mem) │
              │  • Run History      │
              └─────────────────────┘
```

## Why Event-Triggered Automation?

Modern SaaS platforms receive events from dozens of sources — payment processors, version control systems, helpdesk tools. Orchestrating responses to these events reliably is hard:

- **Duplicate events** are inevitable in distributed systems (network retries, at-least-once delivery). Without idempotency, you double-charge customers or send duplicate notifications.
- **Transient failures** (timeouts, 5xx errors) should be retried with backoff, not fail immediately.
- **Permanent failures** need a safety net — a dead-letter queue where you can inspect and replay failed events later.
- **Debugging** event flows across multiple services is painful without structured logs and run history.

ETAA solves all four with a single Python library.

## Core Concepts

### EventEnvelope

The canonical event shape that flows through the entire system. All ingress adapters (webhooks, queues) normalise their raw payloads into this model:

```python
from etaa.models import EventEnvelope

event = EventEnvelope(
    event_id="evt_stripe_abc123",
    source="stripe",
    type="payment_succeeded",
    payload={"amount": 999, "currency": "usd", "customer_id": "cus_xyz"},
)
```

Key fields:
- `event_id` — unique per-event UUID (used for idempotency)
- `source` — originating system (`"stripe"`, `"github"`, custom)
- `type` — event type (`"payment_succeeded"`, `"push"`, etc.)
- `payload` — arbitrary JSON body

### WorkflowSpec

A declarative YAML file describing *when* to run and *what* to do:

```yaml
id: payment-webhook
name: Payment Webhook Processing
triggers:
  - source: stripe
    type: payment_succeeded
    filters:
      success: true
actions:
  - id: notify_email
    type: http
    config:
      method: POST
      url: "https://api.example.com/email"
      body: { to: "{{payload.customer_email}}", subject: "Payment received" }
    retry_policy:
      max_attempts: 3
      backoff_strategy: exponential
      backoff_seconds: 1.0
  - id: update_crm
    type: function
    config:
      module_path: myapp.crm.update
    depends_on: [notify_email]
dlq:
  target_type: file
  target_config: { path: ./data/dlq/payments.jsonl }
```

Matching logic: **OR** between triggers, **AND** between source/type/filters within a trigger.

### Idempotency

ETAA tracks `(workflow_id, event_id)` pairs. If the same event arrives twice for the same workflow, the second run is marked `skipped` — no actions execute, no side effects occur.

Default TTL: 1 hour (configurable). For long-running systems, swap the in-memory store for Redis or a database.

### Retry & DLQ

Each action (or the whole workflow) gets a `RetryPolicy`:

| Field | Default | Description |
|---|---|---|
| `max_attempts` | 3 | Total tries before giving up |
| `backoff_strategy` | `exponential` | `fixed` or `exponential` |
| `backoff_seconds` | 1.0 | Base delay; exponential doubles each attempt (capped at 60s) |
| `retryable_error_codes` | all | List of status codes / strings that warrant a retry; empty list = retry everything |

When retries are exhausted, the event is routed to the `DLQConfig` target:
- `file` — append to a JSONL file (default, great for debugging)
- `queue` — push to SQS / RabbitMQ (TODO: real adapter)
- `db` — insert into a database table (TODO: real adapter)

## Architecture

| Module | Responsibility |
|---|---|
| `etaa/models/` | Data schemas (`EventEnvelope`, `WorkflowSpec`, `RetryPolicy`, …) |
| `etaa/engine/` | Core loop: `EventRouter` → `WorkflowExecutor` with idempotency + retry + DLQ |
| `etaa/ingress/` | Adapters that turn raw HTTP/queue messages into `EventEnvelope` |
| `etaa/actions/` | Action handlers: HTTP, queue publish, local function call, AI stub |
| `etaa/observability/` | JSON logger, in-memory metrics, SQLite/JSONL run history |

## Quickstart

### Install

```bash
git clone https://github.com/AshraHossain/event-triggered-automation-agent.git
cd event-triggered-automation-agent
uv venv && uv pip install -e ".[dev]"   # or: python -m venv .venv && source .venv/bin/activate && pip install -e ".[dev]"
```

### Run the webhook server

```bash
# Start with all example workflows loaded
python -m etaa.main server --workflows examples/ --port 8000
```

### Send a sample event

```bash
curl -X POST http://localhost:8000/api/v1/webhook/stripe \
  -H "Content-Type: application/json" \
  -d '{
    "id": "evt_test_001",
    "source": "stripe",
    "type": "payment_succeeded",
    "data": {
      "customer_id": "cus_123",
      "customer_email": "alice@example.com",
      "amount": 4999,
      "currency": "usd",
      "invoice_id": "inv-001",
      "success": true
    }
  }'
```

### Check metrics

```bash
curl http://localhost:8000/api/v1/metrics
curl http://localhost:8000/api/v1/runs
```

### CLI: run a single event file

```bash
echo '{"id":"e1","source":"stripe","type":"payment_succeeded","data":{"amount":100,"success":true}}' > /tmp/event.json
python -m etaa.main run /tmp/event.json --workflows examples/payment_webhook/
```

> The payment workflow trigger filters on `success: true`, so the event must include
> it to match.

## Examples

| Example | Trigger | Actions |
|---|---|---|
| `examples/payment_webhook/` | Stripe `payment_succeeded` | HTTP email + CRM function |
| `examples/gitops_automation/` | GitHub `push` to `main` | CI function + Slack HTTP |
| `examples/support_ticket_flow/` | `ticket_created` | AI classify → assign → email |

Run any demo:
```bash
PYTHONPATH=. python examples/payment_webhook/demo.py
```

> Demo workflows POST to placeholder URLs, so without a reachable endpoint the HTTP
> actions fail and runs end in `failed`/`dlq` — this is intentional and exercises the
> retry and dead-letter paths.

## Benchmarks

```bash
PYTHONPATH=. python benchmark.py --workflows examples/ --iterations 10
```

Outputs a Markdown table + `benchmark_results.json` with per-workflow latency and throughput numbers.

## Security Configuration

ETAA ships with two opt-in controls, both configured via environment variables:

| Variable | Purpose |
|---|---|
| `ETAA_WEBHOOK_SECRET` | Global HMAC-SHA256 secret for webhook signature verification. |
| `ETAA_WEBHOOK_SECRET_<SOURCE>` | Per-source secret (e.g. `ETAA_WEBHOOK_SECRET_STRIPE`). Takes precedence over the global. |
| `ETAA_WEBHOOK_UNSIGNED_SOURCES` | Comma-separated sources allowed to skip signing. |
| `ETAA_FUNCTION_ALLOWLIST` | Comma-separated module prefixes the `function` action may import (default `examples.,etaa.`). |

When any webhook secret is configured, requests must carry a valid
`X-ETAA-Signature: sha256=<hex>` header; sources without a usable secret are rejected
(**fail closed**) unless listed in `ETAA_WEBHOOK_UNSIGNED_SOURCES`. The signature is an
HMAC-SHA256 of the raw request body, compared in constant time.

```bash
# Example: compute a signature for a payload
SECRET=shh
BODY='{"id":"e1","type":"payment_succeeded","data":{"success":true}}'
SIG="sha256=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print $2}')"
curl -X POST http://localhost:8000/api/v1/webhook/stripe \
  -H "Content-Type: application/json" -H "X-ETAA-Signature: $SIG" -d "$BODY"
```

## Contribution Guide

### Add a new action type

1. Subclass `BaseAction` in `etaa/actions/__init__.py`.
2. Implement `async def execute(self, action: ActionSpec, event: EventEnvelope) -> ActionResult`.
3. Register it in `etaa/main.py` inside `action_registry`.

### Add a new ingress adapter

1. Subclass `BaseIngress` in `etaa/ingress/__init__.py`.
2. Implement `async def ingest(self, raw) -> EventEnvelope`.
3. Wire it into the FastAPI app in `etaa/ingress/webhook_server.py`.

### Extend idempotency / DLQ

- Swap `IdempotencyManager` for a Redis-backed implementation.
- Swap `DLQConfig.target_type` adapters in `retry_dlq_manager.py` for real SQS / Kafka clients.

## License

MIT
