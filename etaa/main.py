"""CLI entrypoint and main application factory for ETAA."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

import yaml

from .actions import AiAction, FunctionAction, HttpAction, QueueAction
from .engine.event_router import EventRouter
from .engine.idempotency_manager import get_idempotency_manager
from .engine.workflow_executor import WorkflowExecutor
from .ingress import WebhookIngress
from .ingress.webhook_server import create_app
from .models import WorkflowSpec
from .observability.metrics_store import get_metrics_store
from .observability.run_history_store import get_run_history


def build_application(
    workflows_path: str | None = None,
) -> tuple[Any, EventRouter, WorkflowExecutor]:
    """Build the full ETAA application with all components wired together.

    Parameters
    ----------
    workflows_path : str, optional
        Directory containing YAML workflow definitions. If given, all *.yaml/*.yml
        files in that directory are loaded automatically.

    Returns
    -------
    app, router, executor
    """

    metrics = get_metrics_store()
    history = get_run_history()
    idempotency = get_idempotency_manager()
    router = EventRouter()

    action_registry: dict[str, Any] = {
        "http": HttpAction(),
        "queue": QueueAction(),
        "function": FunctionAction(),
        "ai": AiAction(),
    }

    def action_factory(kind: str):
        return action_registry.get(kind)

    executor = WorkflowExecutor(router, action_factory, metrics, history, idempotency)

    if workflows_path:
        load_workflows(workflows_path, router)

    app = create_app(executor, router)
    return app, router, executor


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
        # First top-level, then recursive
        for f in sorted(p.glob("*.yaml")) + sorted(p.glob("*.yml")):
            specs.append(_load_single(f))
        for f in sorted(p.rglob("*.yaml")) + sorted(p.rglob("*.yml")):
            if f not in specs:  # avoid duplicates if rglob picks up top-level too
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

    # server
    srv = sub.add_parser("server", help="Start the webhook HTTP server")
    srv.add_argument("--host", default="127.0.0.1")
    srv.add_argument("--port", type=int, default=8000)
    srv.add_argument("--workflows", help="Directory with workflow YAML files")

    # run
    run = sub.add_parser("run", help="Process a single event JSON file")
    run.add_argument("event_file", help="Path to JSON event file")
    run.add_argument("--workflows", required=True, help="Directory with workflow YAML files")

    # list
    lst = sub.add_parser("list", help="List registered workflows")
    lst.add_argument("--workflows", required=True, help="Directory with workflow YAML files")

    # metrics
    sub.add_parser("metrics", help="Print current metrics snapshot")

    args = parser.parse_args()

    if args.command == "server":
        _cmd_server(args)
    elif args.command == "run":
        asyncio.run(_cmd_run(args))
    elif args.command == "list":
        _cmd_list(args)
    elif args.command == "metrics":
        _cmd_metrics()


def _cmd_server(args: argparse.Namespace) -> None:
    import uvicorn
    app, _, _ = build_application(workflows_path=getattr(args, "workflows", None))
    uvicorn.run(app, host=args.host, port=args.port)


async def _cmd_run(args: argparse.Namespace) -> None:
    app, router, executor = build_application(workflows_path=args.workflows)
    event_data = json.loads(Path(args.event_file).read_text())
    ingress = WebhookIngress()
    event = await ingress.ingest(event_data)
    runs = await executor.execute(event)
    for r in runs:
        print(json.dumps(r.model_dump(), default=str, indent=2))


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
