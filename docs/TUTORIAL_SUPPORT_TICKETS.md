# Tutorial: Support Ticket Automation

## Overview

When a support ticket is created, ETAA classifies it (AI action), assigns an owner (local
function), and sends an acknowledgement email (HTTP). The classification from the first
action is passed to the other two, which shows `depends_on` ordering and step outputs.

## Key config

```yaml
actions:
  - id: classify_ticket
    type: ai
    config:
      provider: stub                      # offline keyword rules; or anthropic / openai
      prompt: "Classify this support ticket.\nTicket: {{payload.description}}"
      labels: [billing, technical, account, general]
      default_label: general
      stub_rules:
        billing: [charged, invoice, refund, payment]
        account: [password, login, sign in]

  - id: assign_owner
    type: function
    config: { module_path: examples.support_ticket_flow.handlers.assign_owner }
    depends_on: [classify_ticket]         # reads context["steps"]["classify_ticket"]

  - id: send_ack_email
    type: http
    depends_on: [assign_owner]
    config:
      url: "https://api.example.com/emails/send"
      body:
        to: "{{payload.requester_email}}"
        body: "Categorised as {{steps.classify_ticket.classification}}, assigned to {{steps.assign_owner.result.assigned_to}}."
```

Whatever the provider returns, the result is forced into `labels` (or `default_label`), so
text injected through the ticket description cannot put an arbitrary value into the email.

## Run

```bash
PYTHONPATH=. uv run python examples/support_ticket_flow/demo.py
```

## Expected behaviour

1. `classify_ticket` — "can't reset my password" → `account`; "charged twice" → `billing`.
2. `assign_owner` — routes `account` to `alice-support` and `billing` to `carol-billing`.
3. `send_ack_email` — sent to the demo's mock endpoint; both runs end in `success`.

If the email step fails after its retries, the event is written to the SQLite dead-letter
table `dlq_support_tickets` in `./data/dlq/support.db`. Replay it with:

```bash
uv run etaa replay data/dlq/support.db --table dlq_support_tickets \
  --workflows examples/support_ticket_flow
```

To use a real model, set `provider: anthropic` (with `ANTHROPIC_API_KEY`) or
`provider: openai` (with `OPENAI_API_KEY`) and install the extras: `uv pip install -e ".[ai]"`.
