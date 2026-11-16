"""Workflow executor — runs matched workflows with idempotency, ordering, and error handling."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from ..actions import ActionError, ActionResult, BaseAction
from ..models import (
    ActionSpec,
    AttemptRecord,
    EventEnvelope,
    ExecutionStatus,
    RunRecord,
    WorkflowSpec,
)
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
        if self._idempotency.check_and_set(wf.id, event.event_id):
            log.info("Idempotency hit — skipping %s for event %s", wf.id, event.event_id)
            return self._record_skipped(wf, event)

        run = RunRecord(
            run_id=str(uuid.uuid4()),
            event_id=event.event_id,
            workflow_id=wf.id,
            status=ExecutionStatus.RUNNING,
        )
        self._history.save(run)  # persist initial state
        self._metrics.increment(wf.id, "started_count")

        wf_start = datetime.now(UTC)
        try:
            ordered = self._topological_sort(wf.actions)
        except ValueError as exc:
            log.error("Workflow %s has an invalid dependency graph: %s", wf.id, exc)
            return self._finalize(run, wf, [], 0, ExecutionStatus.FAILED, wf_start)

        status, attempts, logs = await self._run_actions(wf, event, ordered)
        return self._finalize(run, wf, logs, attempts, status, wf_start)

    def _record_skipped(self, wf: WorkflowSpec, event: EventEnvelope) -> RunRecord:
        """Build, persist, and return a SKIPPED run for a duplicate event."""
        run = RunRecord(
            run_id=str(uuid.uuid4()),
            event_id=event.event_id,
            workflow_id=wf.id,
            status=ExecutionStatus.SKIPPED,
            started_at=datetime.now(UTC),
        )
        run.finish(ExecutionStatus.SKIPPED)
        self._history.save(run)
        self._metrics.increment(wf.id, "skipped_count")
        return run

    async def _run_actions(
        self, wf: WorkflowSpec, event: EventEnvelope, ordered: list[ActionSpec]
    ) -> tuple[ExecutionStatus, int, list[AttemptRecord]]:
        """Run *ordered* actions, stopping on the first failure."""
        total_attempts = 0
        status = ExecutionStatus.SUCCESS
        logs: list[AttemptRecord] = []

        for action in ordered:
            policy = action.retry_policy or wf.retry_policy
            result = await self._retry_mgr.run_with_retry(
                action=action,
                event=event,
                executor=self._execute_action,
                policy=policy,
                dlq=wf.dlq,
            )
            total_attempts += result.attempt
            logs.append(result)

            if result.status == "failed":
                status = ExecutionStatus.FAILED
                log.error(
                    "Workflow %s action %s failed after %d attempt(s): %s",
                    wf.id, action.id, result.attempt, result.error,
                )
                break  # stop on first failure
            self._metrics.increment(wf.id, "action_success_count")

        return status, total_attempts, logs

    def _finalize(
        self,
        run: RunRecord,
        wf: WorkflowSpec,
        logs: list[AttemptRecord],
        attempts: int,
        status: ExecutionStatus,
        wf_start: datetime,
    ) -> RunRecord:
        """Attach metrics, persist, and return the completed run."""
        elapsed = (datetime.now(UTC) - wf_start).total_seconds()
        run.attempts = attempts
        run.action_logs = logs
        run.metrics = {
            "latency_seconds": round(elapsed, 4),
            "actions_executed": len(logs),
            "total_attempts": attempts,
        }
        self._metrics.record_latency(wf.id, elapsed)
        self._metrics.increment(wf.id, "completed_count")
        run.finish(status)
        self._history.save(run)

        counter = "success_count" if status == ExecutionStatus.SUCCESS else "failure_count"
        self._metrics.increment(wf.id, counter)
        return run

    async def _execute_action(self, action: ActionSpec, event: EventEnvelope) -> ActionResult:
        """Dispatch to the appropriate action handler."""
        handler = self._action_factory(action.type.value)
        if handler is None:
            raise ActionError(
                f"No handler registered for action type: {action.type.value}",
                error_code="unknown_type",
            )
        return await handler.execute(action, event)

    @staticmethod
    def _topological_sort(actions: list[ActionSpec]) -> list[ActionSpec]:
        """Return *actions* in dependency order (Kahn's algorithm).

        Raises:
            ValueError: if the ``depends_on`` graph contains a cycle.
        """
        id_set = {a.id for a in actions}
        in_degree: dict[str, int] = {a.id: 0 for a in actions}
        children: dict[str, list[str]] = {a.id: [] for a in actions}
        by_id = {a.id: a for a in actions}

        for a in actions:
            for dep in a.depends_on:
                if dep not in id_set:
                    continue
                children[dep].append(a.id)
                in_degree[a.id] += 1

        queue = [aid for aid, deg in in_degree.items() if deg == 0]
        ordered: list[ActionSpec] = []
        while queue:
            node = queue.pop(0)
            ordered.append(by_id[node])
            for child in children[node]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(ordered) != len(actions):
            raise ValueError("dependency cycle detected in action graph")
        return ordered
