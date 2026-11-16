"""Demo script for the gitops_automation workflow."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etaa.main import build_application

SAMPLE_EVENTS = [
    {
        "id": "evt_git_001",
        "source": "github",
        "type": "push",
        "data": {
            "repository": "myorg/webapp",
            "ref": "refs/heads/main",
            "head_commit": {
                "id": "abc123def456",
                "message": "fix: resolve login bug",
                "author": {"name": "Jane Dev"},
            },
        },
    },
    {
        "id": "evt_git_002",
        "source": "github",
        "type": "push",
        "data": {
            "repository": "myorg/webapp",
            "ref": "refs/heads/feature/auth",  # should NOT match (not main)
            "head_commit": {
                "id": "xyz789",
                "message": "wip auth changes",
                "author": {"name": "John Doe"},
            },
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
        if not runs:
            print("  No matching workflows.")
            continue
        for run in runs:
            print(f"  Run {run.run_id}")
            print(f"    Status : {run.status.value}")
            print(f"    Attempts: {run.attempts}")
            for alog in run.action_logs:
                tag = "✓" if alog.status == "success" else "✗"
                print(
                    f"    [{tag}] action={alog.action_id} attempt={alog.attempt} err={alog.error}"
                )
            print(f"    Latency: {run.metrics.get('latency_seconds', 0):.4f}s")

    print("\n" + "=" * 60)
    from etaa.observability.metrics_store import get_metrics_store

    store = get_metrics_store()
    print("Metrics:", json.dumps(store.snapshot(), indent=2, default=str))
    store.close()


if __name__ == "__main__":
    asyncio.run(main())
