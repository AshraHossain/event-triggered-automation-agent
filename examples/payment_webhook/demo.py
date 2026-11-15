"""Demo script for the payment_webhook workflow."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etaa.main import build_application


SAMPLE_EVENTS = [
    {
        "id": "evt_pay_001",
        "source": "stripe",
        "type": "payment_succeeded",
        "data": {
            "customer_id": "cus_123",
            "customer_email": "alice@example.com",
            "amount": 9999,
            "currency": "usd",
            "invoice_id": "inv-456",
            "success": True,
        },
    },
    {
        "id": "evt_pay_002",
        "source": "stripe",
        "type": "payment_succeeded",
        "data": {
            "customer_id": "cus_789",
            "customer_email": "bob@example.com",
            "amount": 4500,
            "currency": "eur",
            "invoice_id": "inv-789",
            "success": True,
        },
    },
    # Duplicate of evt_pay_001 — should be idempotently skipped
    {
        "id": "evt_pay_001",
        "source": "stripe",
        "type": "payment_succeeded",
        "data": {
            "customer_id": "cus_123",
            "customer_email": "alice@example.com",
            "amount": 9999,
            "currency": "usd",
            "invoice_id": "inv-456",
            "success": True,
        },
    },
]


async def main() -> None:
    workflows_dir = Path(__file__).parent
    app, router, executor = build_application(workflows_path=str(workflows_dir))

    from etaa.ingress import WebhookIngress
    ingress = WebhookIngress()

    print(f"Registered workflows: {router.workflow_count}")
    print("=" * 60)

    for idx, raw_event in enumerate(SAMPLE_EVENTS, 1):
        print(f"\n── Event #{idx}: {raw_event['id']} ──")
        event = await ingress.ingest(raw_event)
        runs = await executor.execute(event)
        for run in runs:
            print(f"  Run {run.run_id}")
            print(f"    Status : {run.status.value}")
            print(f"    Attempts: {run.attempts}")
            if run.action_logs:
                for alog in run.action_logs:
                    tag = "✓" if alog.status == "success" else "✗"
                    print(f"    [{tag}] action={alog.action_id} attempt={alog.attempt} err={alog.error}")
            print(f"    Latency: {run.metrics.get('latency_seconds', 0):.4f}s")

    print("\n" + "=" * 60)
    print("Metrics snapshot:")
    from etaa.observability.metrics_store import get_metrics_store
    store = get_metrics_store()
    print(json.dumps(store.snapshot(), indent=2, default=str))
    store.close()

    print("\nRun history:")
    from etaa.observability.run_history_store import get_run_history
    hist = get_run_history()
    for rec in hist.list(limit=10):
        print(f"  {rec.run_id}  wf={rec.workflow_id}  status={rec.status.value}  attempts={rec.attempts}")
    hist.close()


if __name__ == "__main__":
    asyncio.run(main())
