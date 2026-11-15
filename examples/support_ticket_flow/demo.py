"""Demo script for the support_ticket_flow workflow."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etaa.main import build_application


SAMPLE_EVENTS = [
    {
        "id": "evt_ticket_001",
        "source": "support_system",
        "type": "ticket_created",
        "data": {
            "ticket_id": "TKT-1001",
            "requester_email": "customer1@example.com",
            "description": "I can't reset my password. The link doesn't work.",
        },
    },
    {
        "id": "evt_ticket_002",
        "source": "support_system",
        "type": "ticket_created",
        "data": {
            "ticket_id": "TKT-1002",
            "requester_email": "customer2@example.com",
            "description": "I was charged twice for my subscription.",
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
            for alog in run.action_logs:
                tag = "✓" if alog.status == "success" else "✗"
                print(f"    [{tag}] action={alog.action_id} attempt={alog.attempt} err={alog.error}")
            print(f"    Latency: {run.metrics.get('latency_seconds', 0):.4f}s")

    print("\n" + "=" * 60)
    from etaa.observability.metrics_store import get_metrics_store
    store = get_metrics_store()
    print("Metrics:", json.dumps(store.snapshot(), indent=2, default=str))
    store.close()


if __name__ == "__main__":
    asyncio.run(main())
