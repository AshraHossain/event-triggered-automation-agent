"""Demo: payment webhook → email notification → CRM update.

Shows two successful payments, an idempotently skipped duplicate, and one
payment whose email call keeps failing: it is retried with backoff and then
dead-lettered. Replay it afterwards with:

    etaa replay data/dlq/payment.jsonl --workflows examples/payment_webhook --dry-run
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from examples.demo_support import run_demo  # noqa: E402


def _payment(event_id: str, customer: str, email: str, amount: int) -> dict:
    return {
        "id": event_id,
        "source": "stripe",
        "type": "payment_succeeded",
        "data": {
            "customer_id": customer,
            "customer_email": email,
            "amount": amount,
            "currency": "usd",
            "invoice_id": f"inv-{event_id[-3:]}",
            "success": True,
        },
    }


SAMPLE_EVENTS = [
    _payment("evt_pay_001", "cus_123", "alice@example.com", 9999),
    _payment("evt_pay_002", "cus_789", "bob@example.com", 4500),
    _payment("evt_pay_001", "cus_123", "alice@example.com", 9999),  # duplicate → skipped
    _payment("evt_pay_003", "cus_555", "fail@example.com", 1200),  # email down → DLQ
]

if __name__ == "__main__":
    asyncio.run(run_demo(Path(__file__).parent, SAMPLE_EVENTS))
