"""CLI entrypoint and main application factory for ETAA."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any

import httpx
import yaml

from .actions import AiAction, FunctionAction, HttpAction, QueueAction
from .engine import dlq as dlq_store
from .engine.event_router import EventRouter
from .engine.idempotency_manager import get_idempotency_manager
from .engine.state_store import CheckpointStore, Inbox
from .engine.workflow_executor import WorkflowExecutor
from .ingress import WebhookIngress
from .ingress.webhook_server import create_app
from .models import EventEnvelope, ExecutionStatus, WorkflowSpec
from .observability.metrics_store import get_metrics_store
from .observability.run_history_store import get_run_history


def dry_run_transport() -> httpx.MockTransport:
    """Transport that answers every outbound request with 200 and sends nothing."""

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"dry_run": True, "method": request.method, "url": str(request.url)}
        )

    return httpx.MockTransport(_handler)


def build_application(
    workflows_path: str | None = None,
    *,
    dry_run: bool = False,
    http_transport: httpx.AsyncBaseTransport | None = None,
) -> tuple[Any, EventRouter, WorkflowExecutor]:
    """Build the full ETAA application with all components wired together.

    Parameters
    ----------
    workflows_path:
        File or directory of YAML workflow definitions to load (recursive).
    dry_run:
        Render and validate HTTP actions but do not send them.
    http_transport:
        Custom httpx transport for HTTP actions (demos, tests).

    Returns
    -------
    app, router, executor
    """
    transport = dry_run_transport() if dry_run else http_transport
    router = EventRouter()
    action_registry: dict[str, Any] = {
        "http": HttpAction(transport=transport),
        "queue": QueueAction(),
        "function": FunctionAction(),
        "ai": AiAction(),
    }
    # One SQLite file holds idempotency claims, step checkpoints and the inbox.
    state_db = os.environ.get("ETAA_STATE_DB")
    executor = WorkflowExecutor(
        router,
        action_registry.get,
        get_metrics_store(),
        get_run_history(),
        get_idempotency_manager(),
        checkpoints=CheckpointStore(state_db),
    )
    if workflows_path:
        load_workflows(workflows_path, router)
    return create_app(executor, router, inbox=Inbox(state_db)), router, executor


def load_workflows(path: str, router: EventRouter) -> list[WorkflowSpec]:
    """Load all YAML workflow specs from *path* and register them.

    Scans both top-level *.yaml/*.yml files and recursively finds YAML files
    in any subdirectories (so passing ``examples/`` picks up all example workflows).
    """
    p = Path(path)
    specs: list[WorkflowSpec] = []
    if p.is_file():
        specs.append(_load_single(p))
    elif p.is_dir():
        # rglob already covers the top level; dedupe by resolved path so a file
        # is never loaded (and its workflow registered) twice.
        seen: set[Path] = set()
        for f in sorted(p.rglob("*.yaml")) + sorted(p.rglob("*.yml")):
            resolved = f.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            specs.append(_load_single(f))
    else:
        raise FileNotFoundError(f"Workflows path not found: {path}")

    for spec in specs:
        router.add_workflow(spec)
    return specs


def _load_single(path: Path) -> WorkflowSpec:
    text = path.read_text(encoding="utf-8")
    data = yaml.safe_load(text)
    return WorkflowSpec.model_validate(data)


# ─── CLI ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(prog="etaa", description="Event-Triggered Automation Agent")
    sub = parser.add_subparsers(dest="command", required=True)
    dry_help = "Render and validate HTTP actions without sending them"

    srv = sub.add_parser("server", help="Start the webhook HTTP server")
    srv.add_argument("--host", default="127.0.0.1")
    srv.add_argument("--port", type=int, default=8000)
    srv.add_argument("--workflows", help="Directory with workflow YAML files")
    srv.add_argument("--dry-run", action="store_true", help=dry_help)

    run = sub.add_parser("run", help="Process a single event JSON file")
    run.add_argument("event_file", help="Path to JSON event file")
    run.add_argument("--workflows", required=True, help="Directory with workflow YAML files")
    run.add_argument("--dry-run", action="store_true", help=dry_help)

    rep = sub.add_parser("replay", help="Re-run dead-lettered events")
    rep.add_argument("dlq_path", help="DLQ store: a .jsonl file or a SQLite .db")
    rep.add_argument("--workflows", required=True, help="Directory with workflow YAML files")
    rep.add_argument("--table", default=dlq_store.DEFAULT_TABLE, help="Table name (SQLite only)")
    rep.add_argument("--dry-run", action="store_true", help=dry_help)

    val = sub.add_parser("validate", help="Check workflow files without running them")
    val.add_argument("path", help="Workflow YAML file or directory")

    lst = sub.add_parser("list", help="List registered workflows")
    lst.add_argument("--workflows", required=True, help="Directory with workflow YAML files")

    sub.add_parser("metrics", help="Print current metrics snapshot")

    args = parser.parse_args()
    if args.command == "server":
        _cmd_server(args)
    elif args.command == "run":
        asyncio.run(_cmd_run(args))
    elif args.command == "replay":
        asyncio.run(_cmd_replay(args))
    elif args.command == "validate":
        raise SystemExit(_cmd_validate(args))
    elif args.command == "list":
        _cmd_list(args)
    elif args.command == "metrics":
        _cmd_metrics()


def _cmd_server(args: argparse.Namespace) -> None:
    import uvicorn

    app, _, _ = build_application(workflows_path=args.workflows, dry_run=args.dry_run)
    uvicorn.run(app, host=args.host, port=args.port)


async def _cmd_run(args: argparse.Namespace) -> None:
    _, _, executor = build_application(workflows_path=args.workflows, dry_run=args.dry_run)
    event_data = json.loads(Path(args.event_file).read_text(encoding="utf-8"))
    event = await WebhookIngress().ingest(event_data)
    runs = await executor.execute(event)
    if not runs:
        print(f"No workflow matched event {event.event_id} ({event.source}:{event.type})")
    for r in runs:
        print(json.dumps(r.model_dump(mode="json"), indent=2))


async def replay_dlq(
    executor: WorkflowExecutor, dlq_path: str, table: str = dlq_store.DEFAULT_TABLE
) -> dict[str, int]:
    """Re-run every dead-lettered event against the workflow that failed it.

    Processed records are removed afterwards; an event that fails again is
    dead-lettered again by the normal path, so nothing is lost. The workflow is
    re-run from its first action, so actions must be safe to repeat.
    """
    records = dlq_store.load(dlq_path, table)
    counts = {"replayed": 0, "succeeded": 0, "failed": 0, "skipped": 0}
    for record in records:
        evt = record.get("event", {})
        wf = executor.router.get_workflow(record.get("workflow_id", ""))
        if wf is None or "payload" not in evt:
            counts["skipped"] += 1  # unknown workflow, or stored without payload
            continue
        event = EventEnvelope(
            event_id=evt["event_id"],
            source=evt["source"],
            type=evt["type"],
            payload=evt["payload"],
        )
        result = await executor.run_workflow(wf, event)
        if result.status == ExecutionStatus.SKIPPED:
            counts["skipped"] += 1  # already processed (for example a duplicate dead letter)
            continue
        counts["replayed"] += 1
        counts["succeeded" if result.status == ExecutionStatus.SUCCESS else "failed"] += 1
    dlq_store.drop_first(dlq_path, len(records), table)
    return counts


async def _cmd_replay(args: argparse.Namespace) -> None:
    _, _, executor = build_application(workflows_path=args.workflows, dry_run=args.dry_run)
    counts = await replay_dlq(executor, args.dlq_path, args.table)
    print(json.dumps(counts, indent=2))


def validate_workflows(path: str) -> list[str]:
    """Return one error message per invalid workflow file under *path* (empty = all valid)."""
    root = Path(path)
    if not root.exists():
        return [f"{path}: not found"]
    files = [root] if root.is_file() else sorted({*root.rglob("*.yaml"), *root.rglob("*.yml")})
    errors: list[str] = []
    seen: dict[str, Path] = {}
    for file in files:
        try:
            spec = _load_single(file)
        except (yaml.YAMLError, ValueError) as exc:
            errors.append(f"{file}: {exc}")
            continue
        if spec.id in seen:
            errors.append(f"{file}: duplicate workflow id {spec.id!r} (also in {seen[spec.id]})")
        seen[spec.id] = file
    return errors


def _cmd_validate(args: argparse.Namespace) -> int:
    errors = validate_workflows(args.path)
    for error in errors:
        print(f"INVALID  {error}")
    if not errors:
        print(f"OK  all workflows under {args.path} are valid")
    return 1 if errors else 0


def _cmd_list(args: argparse.Namespace) -> None:
    _, router, _ = build_application(workflows_path=args.workflows)
    for wf in router.list_workflows():
        print(f"  {wf.id:30s}  {wf.name}")


def _cmd_metrics() -> None:
    store = get_metrics_store()
    print(json.dumps(store.snapshot(), indent=2, default=str))
    store.close()


def serve() -> None:
    """Entry-point script for ``etaa-server``."""
    main()


if __name__ == "__main__":
    main()
