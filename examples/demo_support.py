"""Shared plumbing for the example demos.

The demos never touch the network: HTTP actions go through a mock transport
that answers 200, except for requests whose body mentions ``fail@`` — those get
a 503 so each demo can show the retry → dead-letter path on purpose.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx

from etaa.ingress import WebhookIngress
from etaa.main import build_application

FAIL_MARKER = b"fail@"


def demo_transport() -> httpx.MockTransport:
    """Mock endpoint: 503 for bodies containing ``fail@``, otherwise 200."""

    def _handler(request: httpx.Request) -> httpx.Response:
        if FAIL_MARKER in request.content:
            return httpx.Response(503, json={"error": "service unavailable"})
        return httpx.Response(200, json={"ok": True})

    return httpx.MockTransport(_handler)


async def run_demo(workflows_dir: Path, events: list[dict[str, Any]]) -> None:
    """Send *events* through the workflows in *workflows_dir* and print each run."""
    _, router, executor = build_application(
        workflows_path=str(workflows_dir), http_transport=demo_transport()
    )
    ingress = WebhookIngress()
    print(f"Registered workflows: {router.workflow_count}")
    print("=" * 60)

    for idx, raw_event in enumerate(events, 1):
        print(f"\n── Event #{idx}: {raw_event['id']} ──")
        runs = await executor.execute(await ingress.ingest(raw_event))
        if not runs:
            print("  No matching workflow.")
        for run in runs:
            print(f"  status={run.status.value}  attempts={run.attempts}")
            for attempt in run.action_logs:
                tag = "✓" if attempt.status == "success" else "✗"
                suffix = f"  error={attempt.error}" if attempt.error else ""
                dlq = "  → dead-lettered" if attempt.dlq_routed else ""
                label = (attempt.response or {}).get("classification")
                note = f"  classification={label}" if label else ""
                print(
                    f"    [{tag}] {attempt.action_id} "
                    f"(attempts={attempt.attempt}){note}{suffix}{dlq}"
                )

    print("\n" + "=" * 60)
    print("Metrics:", json.dumps(executor.metrics.snapshot(), indent=2, default=str))
