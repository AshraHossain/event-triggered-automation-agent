"""Workflow executor — runs matched workflows with idempotency, ordering, and error handling."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from ..actions import ActionError, ActionResult, BaseAction, Steps
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
from .state_store import CheckpointStore

log = setup_logger(__name__)


class WorkflowExecutor:
    """Execute workflows matching an incoming event.

    Responsibilities:
      - Claim the idempotency key (skip duplicates, release on failure)
      - Resolve action dependencies (topological order)
      - Execute each action with retry + DLQ, passing earlier outputs forward
      - Checkpoint each completed action so a retry resumes at the failed one
      - Record run history and metrics

    Example:
        >>> executor = WorkflowExecutor(router, action_factory, metrics, history, idem)
        >>> runs = await executor.execute(event)
    """

    def __init__(
        self,
        router: EventRouter,
        action_factory: Callable[[str], BaseAction | None],
        metrics: MetricsStore,
        history: RunHistoryStore,
        idempotency: IdempotencyManager,
        retry_manager: RetryDLQManager | None = None,
        checkpoints: CheckpointStore | None = None,
    ) -> None:
        self._router = router
        self._action_factory = action_factory
        self._metrics = metrics
        self._history = history
        self._idempotency = idempotency
        self._retry_mgr = retry_manager or RetryDLQManager()
        self._checkpoints = checkpoints or CheckpointStore()

    @property
    def router(self) -> EventRouter:
        return self._router

    @property
    def metrics(self) -> MetricsStore:
        return self._metrics

    @property
    def history(self) -> RunHistoryStore:
        return self._history

    async def execute(self, event: EventEnvelope) -> list[RunRecord]:
        """Run all matching workflows for *event*. Returns list of RunRecords."""
        return [await self.run_workflow(wf, event) for wf in self._router.resolve(event)]

    async def run_workflow(self, wf: WorkflowSpec, event: EventEnvelope) -> RunRecord:
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

        try:
            ordered = self._topological_sort(wf.actions)
        except ValueError as exc:
            log.error("Workflow %s has an invalid dependency graph: %s", wf.id, exc)
            return self._finalize(run, wf, [], ExecutionStatus.FAILED)

        status, logs = await self._run_actions(wf, event, ordered)
        return self._finalize(run, wf, logs, status)

    def _record_skipped(self, wf: WorkflowSpec, event: EventEnvelope) -> RunRecord:
        """Build, persist, and return a SKIPPED run for a duplicate event."""
        now = datetime.now(UTC)
        run = RunRecord(
            run_id=str(uuid.uuid4()),
            event_id=event.event_id,
            workflow_id=wf.id,
            status=ExecutionStatus.SKIPPED,
            started_at=now,
            finished_at=now,
        )
        self._history.save(run)
        self._metrics.increment(wf.id, "skipped_count")
        return run

    async def _run_actions(
        self, wf: WorkflowSpec, event: EventEnvelope, ordered: list[ActionSpec]
    ) -> tuple[ExecutionStatus, list[AttemptRecord]]:
        """Run *ordered* actions, stopping on the first failure."""
        logs: list[AttemptRecord] = []
        # Actions that succeeded in an earlier, failed attempt at this event.
        steps: Steps = self._checkpoints.load(wf.id, event.event_id)

        async def _execute(action: ActionSpec, evt: EventEnvelope) -> ActionResult:
            return await self._execute_action(action, evt, steps)

        for action in ordered:
            if action.id in steps:
                logs.append(self._resumed_record(action.id, steps[action.id]))
                continue
            result = await self._retry_mgr.run_with_retry(
                action=action,
                event=event,
                executor=_execute,
                policy=action.retry_policy or wf.retry_policy,
                dlq=wf.dlq,
                workflow_id=wf.id,
            )
            logs.append(result)
            if result.status == "failed":
                log.error(
                    "Workflow %s action %s failed after %d attempt(s): %s",
                    wf.id,
                    action.id,
                    result.attempt,
                    result.error,
                )
                return ExecutionStatus.FAILED, logs
            steps[action.id] = result.response or {}
            self._checkpoints.save(wf.id, event.event_id, action.id, steps[action.id])
            self._metrics.increment(wf.id, "action_success_count")

        self._checkpoints.clear(wf.id, event.event_id)
        return ExecutionStatus.SUCCESS, logs

    @staticmethod
    def _resumed_record(action_id: str, response: dict) -> AttemptRecord:
        """Log entry for an action skipped because an earlier attempt completed it."""
        now = datetime.now(UTC)
        return AttemptRecord(
            action_id=action_id,
            attempt=0,
            started_at=now,
            finished_at=now,
            status="resumed",
            response=response,
        )

    def _finalize(
        self,
        run: RunRecord,
        wf: WorkflowSpec,
        logs: list[AttemptRecord],
        status: ExecutionStatus,
    ) -> RunRecord:
        """Attach metrics, persist, and return the completed run."""
        finished = datetime.now(UTC)
        elapsed = (finished - run.started_at).total_seconds()
        attempts = sum(log_.attempt for log_ in logs)
        done = run.model_copy(
            update={
                "status": status,
                "finished_at": finished,
                "attempts": attempts,
                "action_logs": logs,
                "metrics": {
                    "latency_seconds": round(elapsed, 4),
                    "actions_executed": len(logs),
                    "total_attempts": attempts,
                },
            }
        )
        self._history.save(done)

        self._metrics.record_latency(wf.id, elapsed)
        self._metrics.increment(wf.id, "completed_count")
        retries = attempts - sum(1 for log_ in logs if log_.attempt > 0)
        if retries > 0:
            self._metrics.increment(wf.id, "retry_count", retries)
        if any(log_.dlq_routed for log_ in logs):
            self._metrics.increment(wf.id, "dlq_count")

        if status == ExecutionStatus.SUCCESS:
            self._metrics.increment(wf.id, "success_count")
        else:
            self._metrics.increment(wf.id, "failure_count")
            # Release the claim so a redelivery or DLQ replay is not skipped.
            self._idempotency.release(wf.id, run.event_id)
        return done

    async def _execute_action(
        self, action: ActionSpec, event: EventEnvelope, steps: Steps
    ) -> ActionResult:
        """Dispatch to the appropriate action handler."""
        handler = self._action_factory(action.type.value)
        if handler is None:
            raise ActionError(
                f"No handler registered for action type: {action.type.value}",
                error_code="unknown_type",
                retryable=False,
            )
        return await handler.execute(action, event, steps)

    @staticmethod
    def _topological_sort(actions: list[ActionSpec]) -> list[ActionSpec]:
        """Return *actions* in dependency order (Kahn's algorithm).

        Raises:
            ValueError: if the ``depends_on`` graph contains a cycle.
        """
        by_id = {a.id: a for a in actions}
        in_degree: dict[str, int] = {a.id: 0 for a in actions}
        children: dict[str, list[str]] = {a.id: [] for a in actions}

        for a in actions:
            for dep in a.depends_on:
                if dep not in by_id:
                    raise ValueError(f"action {a.id!r} depends on unknown action {dep!r}")
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
