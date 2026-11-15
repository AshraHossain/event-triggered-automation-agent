# Tutorial: Support Ticket Automation

## Overview

When a support ticket is created, ETAA automatically classifies it (AI stub), assigns an owner (local function), and sends an acknowledgment email (HTTP). The full three-action chain demonstrates `depends_on` DAG execution.

## Key Config Highlights

```yaml
actions:
  - id: classify_ticket
    type: ai
    config: { provider: stub, model: gpt-4o-mini, prompt: "Classify this ticket..." }
    # no depends_on — runs first

  - id: assign_owner
    type: function
    config: { module_path: examples.support_ticket_flow.handlers.assign_owner }
    depends_on: [classify_ticket]     # needs classification result from context

  - id: send_ack_email
    type: http
    config:
      url: "https://api.example.com/emails/send"
      body: { to: "{{payload.requester_email}}", subject: "We received your ticket" }
    depends_on: [assign_owner]         # runs last
    retry_policy:
      max_attempts: 2
      backoff_strategy: fixed
      backoff_seconds: 1.0
```

## Run

```bash
PYTHONPATH=. python examples/support_ticket_flow/demo.py
```

## Expected Behaviour

1. `classify_ticket` → AI stub returns deterministic hash-based classification.
2. `assign_owner` → routes to `carol-billing` (billing), `bob-tech` (technical), or random pool.
3. `send_ack_email` → HTTP call to fake URL → connection error → workflow `failed`, event sent to DLQ (db target, stubbed).

Note: the DLQ target is `db` type, which is a TODO stub — errors are logged but no actual DB write occurs.
