# Tutorial: Payment Webhook Workflow

This tutorial walks through the complete lifecycle of the `payment_webhook` example: define the workflow, start the server, send events, and inspect results.

## Prerequisites

```bash
git clone <repo-url>
cd Event-Triggered-Automation-Agent
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Step 1 — Understand the Workflow Spec

Open `examples/payment_webhook/workflow.yaml`:

```yaml
id: payment-webhook
name: Payment Webhook Processing
triggers:
  - source: stripe
    type: payment_succeeded
    filters:
      success: true          # only process successful payments
actions:
  - id: notify_email        # 1️⃣  HTTP call to email service
    type: http
    config:
      method: POST
      url: "https://api.example.com/notifications/email"
      body:
        to: "{{payload.customer_email}}"
        subject: "Payment received — Invoice #{{payload.invoice_id}}"
    retry_policy:
      max_attempts: 3
      backoff_strategy: exponential
      backoff_seconds: 1.0
      retryable_error_codes: [429, 500, 502, 503, "timeout"]

  - id: update_crm          # 2️⃣  Local function (runs after email succeeds)
    type: function
    config:
      module_path: examples.payment_webhook.handlers.update_crm
    depends_on: [notify_email]

dlq:
  target_type: file
  target_config: { path: ./data/dlq/payment.jsonl }
```

Key observations:
- The workflow fires only on `source=stripe` + `type=payment_succeeded` + `payload.success=true`.
- `notify_email` is retried up to 3 times with exponential backoff (1s, 2s, 4s).
- `update_crm` runs only if `notify_email` succeeds (`depends_on`).
- Failed events land in `./data/dlq/payment.jsonl`.

## Step 2 — Run the Demo Script

The demo sends three events: two distinct payments and one duplicate (to test idempotency).

```bash
PYTHONPATH=. python examples/payment_webhook/demo.py
```

Expected output:

```
Registered workflows: 1
============================================================

── Event #1: evt_pay_001 ──
  Run <uuid>
    Status : failed                 ← HTTP call to fake URL fails (expected in demo)
    Attempts: 1
    [✗] action=notify_email attempt=1 err=Connection error: ...
    Latency: 0.06s

── Event #2: evt_pay_002 ──
  Run <uuid>
    Status : failed
    [✗] action=notify_email attempt=1 ...

── Event #3: evt_pay_001 ──        ← Duplicate!
  Run <uuid>
    Status : skipped                ← Idempotency kicks in — zero work done
    Attempts: 0

Metrics snapshot:
{
  "payment-webhook": {
    "started_count": 2,
    "skipped_count": 1,
    "failure_count": 2,
    "latency_avg": 0.03
  }
}
```

## Step 3 — Inspect the Dead-Letter Queue

```bash
cat data/dlq/payment.jsonl
```

You'll see JSONL entries like:

```json
{"dlq_enqueued_at":"2026-10-01T01:00:00Z","action_id":"notify_email","event":{"event_id":"evt_pay_001","source":"stripe","type":"payment_succeeded","payload":{...}},"error":"Connection error: ..."}
```

These are your salvageable events. In production, a worker would poll this DLQ and retry with adjusted config.

## Step 4 — Run the Webhook Server (optional)

```bash
PYTHONPATH=. python -m etaa.main server --workflows examples/payment_webhook/ --port 8000
```

In another terminal:

```bash
curl -X POST http://localhost:8000/api/v1/webhook/stripe \
  -H "Content-Type: application/json" \
  -d '{"id":"evt_live_001","source":"stripe","type":"payment_succeeded","data":{"customer_email":"u@example.com","amount":5000,"success":true}}'
```

Check metrics:
```bash
curl http://localhost:8000/api/v1/metrics
curl http://localhost:8000/api/v1/runs
```

## What's Next

- Try `examples/gitops_automation/demo.py` and `examples/support_ticket_flow/demo.py`.
- Run the full benchmark: `python benchmark.py --iterations 20`.
- Read [`docs/IDEMPOTENCY_AND_RETRIES.md`](IDEMPOTENCY_AND_RETRIES.md) for deeper theory.
