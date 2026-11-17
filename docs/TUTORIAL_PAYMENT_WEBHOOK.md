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
- `notify_email` gets up to 3 attempts with exponential backoff between them (1s, then 2s).
- `update_crm` runs only if `notify_email` succeeds (`depends_on`).
- Failed events land in `./data/dlq/payment.jsonl`.

## Step 2 — Run the Demo Script

The demo sends four events through a mock HTTP endpoint (no network needed): two payments,
a duplicate of the first, and one payment whose email call always returns 503.

```bash
PYTHONPATH=. uv run python examples/payment_webhook/demo.py
```

Expected output:

```
── Event #1: evt_pay_001 ──
  status=success  attempts=2
    [✓] notify_email (attempts=1)
    [✓] update_crm (attempts=1)

── Event #2: evt_pay_002 ──
  status=success  attempts=2

── Event #3: evt_pay_001 ──          ← duplicate
  status=skipped  attempts=0         ← idempotency: nothing executed

── Event #4: evt_pay_003 ──          ← email endpoint is down
  status=failed  attempts=3
    [✗] notify_email (attempts=3)  error=Unexpected status 503  → dead-lettered
```

Event #4 takes about three seconds: the email is retried with exponential backoff (1s, 2s)
before the event is dead-lettered. `update_crm` never runs because it depends on the email.

## Step 3 — Inspect and Replay the Dead-Letter Queue

```bash
cat data/dlq/payment.jsonl
```

```json
{"dlq_enqueued_at":"…","workflow_id":"payment-webhook","action_id":"notify_email","event":{"event_id":"evt_pay_003","source":"stripe","type":"payment_succeeded","payload":{…}},"error":"Unexpected status 503"}
```

Once the downstream service is healthy, replay it:

```bash
uv run etaa replay data/dlq/payment.jsonl --workflows examples/payment_webhook --dry-run
# { "replayed": 1, "succeeded": 1, "failed": 0, "skipped": 0 }
```

Replayed records are removed from the file. An event that fails again is dead-lettered
again, so nothing is lost. A replay resumes at the action that failed; actions that already
succeeded are not repeated.

## Step 4 — Run the Webhook Server (optional)

```bash
uv run etaa server --workflows examples/payment_webhook/ --port 8000 --dry-run
```

In another terminal:

```bash
curl -X POST http://localhost:8000/api/v1/webhook/stripe \
  -H "Content-Type: application/json" \
  -d '{"id":"evt_live_001","type":"payment_succeeded","data":{"customer_email":"u@example.com","amount":5000,"success":true}}'

# → 202 {"event_id":"evt_live_001","status":"accepted","workflows":["payment-webhook"]}

curl http://localhost:8000/api/v1/metrics
curl http://localhost:8000/api/v1/runs
```

The `202` means the event was recorded; the workflow runs in the background. Add
`?wait=true` to the webhook URL to wait for the result instead.

`--dry-run` validates and renders the HTTP actions without sending them. See the Security
section of the README for signed requests.

## What's Next

- Try `examples/gitops_automation/demo.py` and `examples/support_ticket_flow/demo.py`.
- Run the benchmark: `make benchmark`.
- Read [`docs/IDEMPOTENCY_AND_RETRIES.md`](IDEMPOTENCY_AND_RETRIES.md) for the reasoning.
