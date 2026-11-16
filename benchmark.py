"""Benchmark / load-test runner for ETAA.

Replays a batch of synthetic events against all registered workflows,
collects metrics, and prints a Markdown + JSON summary.

Usage:
    python benchmark.py --workflows examples/ --iterations 100
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from etaa.ingress import WebhookIngress
from etaa.main import build_application


@dataclass
class WorkflowStats:
    """Accumulated stats for a single workflow across many runs."""

    workflow_id: str
    total_events: int = 0
    success_count: int = 0
    failure_count: int = 0
    dlq_count: int = 0
    skipped_count: int = 0
    latencies: list[float] = field(default_factory=list)
    total_attempts: int = 0


def generate_sample_events() -> list[dict[str, Any]]:
    """Return a diverse batch of sample events covering all three examples."""
    return (
        [
            # Payment events
            {
                "id": f"evt-pay-{i}",
                "source": "stripe",
                "type": "payment_succeeded",
                "data": {
                    "customer_id": f"cus_{i}",
                    "customer_email": f"user{i}@example.com",
                    "amount": 1000 + i * 100,
                    "currency": "usd",
                    "invoice_id": f"inv-{i}",
                    "success": True,
                },
            }
            for i in range(1, 51)
        ]
        + [
            # Git push events
            {
                "id": f"evt-git-{i}",
                "source": "github",
                "type": "push",
                "data": {
                    "repository": "myorg/webapp",
                    "ref": "refs/heads/main",
                    "head_commit": {
                        "id": f"commit{i:08x}",
                        "message": f"fix #{i}",
                        "author": {"name": f"dev{i}"},
                    },
                },
            }
            for i in range(1, 51)
        ]
        + [
            # Support ticket events
            {
                "id": f"evt-tkt-{i}",
                "source": "support_system",
                "type": "ticket_created",
                "data": {
                    "ticket_id": f"TKT-{2000+i}",
                    "requester_email": f"cust{i}@example.com",
                    "description": f"Support request number {i}: something needs fixing.",
                },
            }
            for i in range(1, 51)
        ]
    )


async def run_benchmark(workflows_dir: str, iterations: int) -> None:
    app, router, executor = build_application(workflows_path=workflows_dir)
    ingress = WebhookIngress()
    events = generate_sample_events()

    print(f"Workflows loaded : {router.workflow_count}")
    print(f"Events per round : {len(events)}")
    print(f"Iterations       : {iterations}")
    print("=" * 60)

    all_stats: dict[str, WorkflowStats] = {}
    global_start = time.monotonic()

    for it in range(1, iterations + 1):
        round_start = time.monotonic()
        for event_data in events:
            event = await ingress.ingest(event_data)
            runs = await executor.execute(event)
            for run in runs:
                wf = run.workflow_id
                if wf not in all_stats:
                    all_stats[wf] = WorkflowStats(workflow_id=wf)
                s = all_stats[wf]
                s.total_events += 1
                s.latencies.append(run.metrics.get("latency_seconds", 0.0))
                s.total_attempts += run.attempts
                if run.status.value == "success":
                    s.success_count += 1
                elif run.status.value == "failed":
                    s.failure_count += 1
                elif run.status.value == "dlq":
                    s.dlq_count += 1
                elif run.status.value == "skipped":
                    s.skipped_count += 1
        round_elapsed = time.monotonic() - round_start
        if it % 10 == 0 or it == 1:
            print(f"  Iteration {it}/{iterations}  ({round_elapsed:.2f}s)")

    total_elapsed = time.monotonic() - global_start

    # ── Summary ────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("BENCHMARK RESULTS")
    print("=" * 60)
    total_events = sum(s.total_events for s in all_stats.values())
    rate = total_events / total_elapsed if total_elapsed else 0.0
    print(f"Total wall-time     : {total_elapsed:.2f}s")
    print(f"Total events procs  : {total_events}")
    print(f"Events/sec          : {rate:.1f}")
    print()

    lines = ["## Benchmark Summary", ""]
    lines.append("| Workflow | Events | Success | Failure | DLQ | Skipped | Avg Latency (ms) |")
    lines.append("|----------|--------|---------|---------|-----|---------|-------------------|")

    summary_json: dict[str, Any] = {"total_wall_time_s": round(total_elapsed, 2), "workflows": {}}

    for wf_id, s in sorted(all_stats.items()):
        avg_ms = (sum(s.latencies) / len(s.latencies) * 1000) if s.latencies else 0
        lines.append(
            f"| {wf_id} | {s.total_events} | {s.success_count} | {s.failure_count} "
            f"| {s.dlq_count} | {s.skipped_count} | {avg_ms:.1f} |"
        )
        summary_json["workflows"][wf_id] = {
            "total_events": s.total_events,
            "success_count": s.success_count,
            "failure_count": s.failure_count,
            "dlq_count": s.dlq_count,
            "skipped_count": s.skipped_count,
            "avg_latency_ms": round(avg_ms, 2),
        }

    print("\n".join(lines))
    print()
    print("JSON summary:")
    print(json.dumps(summary_json, indent=2))

    # Save JSON artifact
    out = Path("benchmark_results.json")
    out.write_text(json.dumps(summary_json, indent=2))
    print(f"\nResults saved → {out.resolve()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="ETAA Benchmark Runner")
    parser.add_argument(
        "--workflows", default="examples", help="Directory with workflow YAML files"
    )
    parser.add_argument(
        "--iterations", type=int, default=10, help="Number of full-pass iterations"
    )
    args = parser.parse_args()
    asyncio.run(run_benchmark(args.workflows, args.iterations))


if __name__ == "__main__":
    main()
