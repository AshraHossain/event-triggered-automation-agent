"""Workflow executor — runs matched workflows with idempotency, ordering, and error handling."""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from typing import Any, Awaitable, Callable

from ..actions import ActionError, ActionResult, BaseAction
from ..models import ActionKind, AttemptRecord, EventEnvelope, ExecutionStatus, RunRecord, WorkflowSpec
from ..observability.logger import setup_logger
from ..observability.metrics_store import MetricsStore
from ..observability.run_history_store import RunHistoryStore
from .event_router import EventRouter
from .idempotency_manager import IdempotencyManager
from .retry_dlq_manager import RetryDLQManager

log = setup_logger(__name__)


class WorkflowExecutor:
    """Execute workflows matching an incoming event.

    Responsibilities:
      - Check idempotency (skip duplicates)
      - Resolve action dependencies (topological order)
      - Execute each action with retry + DLQ
      - Record run history and metrics

    Example:
        >>> executor = WorkflowExecutor(router, action_factory, metrics, history)
        >>> run = await executor.execute(event)
        >>> print(run.status, run.attempts)
    """

    def __init__(
        self,
        router: EventRouter,
        action_factory: Callable[[str], BaseAction],
        metrics: MetricsStore,
        history: RunHistoryStore,
        idempotency: IdempotencyManager,
    ) -> None:
        self._router = router
        self._action_factory = action_factory
        self._metrics = metrics
        self._history = history
        self._idempotency = idempotency
        self._retry_mgr = RetryDLQManager()

    async def execute(self, event: EventEnvelope) -> list[RunRecord]:
        """Run all matching workflows for *event*. Returns list of RunRecords."""
        workflows = self._router.resolve(event)
        runs: list[RunRecord] = []

        for wf in workflows:
            run = await self._run_workflow(wf, event)
            runs.append(run)

        return runs

    async def _run_workflow(self, wf: WorkflowSpec, event: EventEnvelope) -> RunRecord:
        """Execute a single workflow for *event*, respecting idempotency."""

        # Idempotency check
        if self._idempotency.check_and_set(wf.id, event.event_id):
            log.info("Idempotency hit — skipping %s for event %s", wf.id, event.event_id)
            run = RunRecord(
                run_id=str(uuid.uuid4()),
                event_id=event.event_id,
                workflow_id=wf.id,
                status=ExecutionStatus.SKIPPED,
                started_at=datetime.utcnow(),
            )
            run.finish(ExecutionStatus.SKIPPED)
            self._history.save(run)
            self._metrics.increment(wf.id, "skipped_count")
            return run

        run_id = str(uuid.uuid4())
        run = RunRecord(
            run_id=run_id,
            event_id=event.event_id,
            workflow_id=wf.id,
            status=ExecutionStatus.RUNNING,
        )
        self._history.save(run)  # persist initial state
        self._metrics.increment(wf.id, "started_count")

        total_attempts = 0
        overall_status = ExecutionStatus.SUCCESS
        action_logs: list[AttemptRecord] = []

        # Topological sort respecting depends_on
        ordered = self._topological_sort(wf.actions)

        wf_start = datetime.utcnow()
        for action in ordered:
            action_start = datetime.utcnow()
            policy = action.retry_policy or wf.retry_policy
            result = await self._retry_mgr.run_with_retry(
                action=action,
                event=event,
                executor=self._execute_action,
                policy=policy,
                dlq=wf.dlq,
            )
            total_attempts += result.attempt
            action_logs.append(result)

            if result.status == "failed":
                overall_status = ExecutionStatus.FAILED
                log.error(
                    "Workflow %s action %s failed after %d attempt(s): %s",
                    wf.id, action.id, result.attempt, result.error,
                )
                break  # stop on first failure

            self._metrics.increment(wf.id, "action_success_count")

        elapsed = (datetime.utcnow() - wf_start).total_seconds()
        run.attempts = total_attempts
        run.action_logs = action_logs
        run.metrics = {
            "latency_seconds": round(elapsed, 4),
            "actions_executed": len(action_logs),
            "total_attempts": total_attempts,
        }
        self._metrics.record_latency(wf.id, elapsed)
        self._metrics.increment(wf.id, "completed_count")
        run.finish(overall_status)
        self._history.save(run)

        if overall_status == ExecutionStatus.SUCCESS:
            self._metrics.increment(wf.id, "success_count")
        else:
            self._metrics.increment(wf.id, "failure_count")

        return run

    async def _execute_action(self, action: ActionSpec, event: EventEnvelope) -> ActionResult:
        """Dispatch to the appropriate action handler."""
        handler = self._action_factory(action.type.value)
        if handler is None:
            raise ActionError(f"No handler registered for action type: {action.type.value}", error_code="unknown_type")
        return await handler.execute(action, event)

    @staticmethod
    def _topological_sort(actions: list) -> list:
        """Sort actions respecting depends_on (Kahn's algorithm)."""
        id_set = {a.id for a in actions}
        in_degree: dict[str, int] = {a.id: 0 for a in actions}
        children: dict[str, list] = {a.id: [] for a in actions}

        for a in actions:
            for dep in a.depends_on:
                if dep not in id_set:
                    continue
                children[dep].append(a.id)
                in_degree[a.id] += 1

        queue = [aid for aid, deg in in_degree.items() if deg == 0]
        ordered: list = []
        while queue:
            node = queue.pop(0)
            ordered.append(next(a for a in actions if a.id == node))
            for child in children[node]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(ordered) != len(actions):
            # Cycle detected — fall back to original order with a warning
            log.warning("Cycle detected in action dependencies for workflow; using original order.")
            return actions
        return ordered
