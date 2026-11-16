"""Retry and DLQ manager.

Applies backoff policies and routes exhausted retries to dead-letter storage.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from ..actions.base import ActionResult
from ..models import ActionSpec, AttemptRecord, DLQConfig, EventEnvelope, RetryPolicy
from ..observability.logger import setup_logger
from . import dlq as dlq_store

log = setup_logger(__name__)

ActionExecutor = Callable[[ActionSpec, EventEnvelope], Awaitable[ActionResult]]


class RetryDLQManager:
    """Orchestrates retries with backoff and final DLQ routing.

    Example:
        >>> mgr = RetryDLQManager()
        >>> record = await mgr.run_with_retry(action, event, executor_fn, policy)
    """

    def __init__(self, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> None:
        # Injectable so tests and demos can skip real backoff delays.
        self._sleep = sleep

    async def run_with_retry(
        self,
        action: ActionSpec,
        event: EventEnvelope,
        executor: ActionExecutor,
        policy: RetryPolicy,
        dlq: DLQConfig | None = None,
        workflow_id: str = "",
    ) -> AttemptRecord:
        """Execute *action* up to ``policy.max_attempts`` with backoff.

        Routes to the DLQ on final failure and returns the final AttemptRecord.
        """
        last_error: str | None = None
        attempt = 0

        for attempt in range(1, policy.max_attempts + 1):
            try:
                result = await executor(action, event)
            except Exception as exc:
                last_error = str(exc)
                error_code = getattr(exc, "error_code", None)
                retryable = getattr(exc, "retryable", True)
                if not retryable or not policy.should_retry(error_code, attempt):
                    log.warning(
                        "Action %s failed (code=%s, retryable=%s) on attempt %d",
                        action.id,
                        error_code,
                        retryable,
                        attempt,
                    )
                    break
                delay = policy.compute_delay(attempt)
                log.info(
                    "Retrying action %s in %.2fs (attempt %d/%d)",
                    action.id,
                    delay,
                    attempt,
                    policy.max_attempts,
                )
                await self._sleep(delay)
            else:
                return AttemptRecord(
                    action_id=action.id,
                    attempt=attempt,
                    started_at=result.executed_at,
                    finished_at=datetime.now(UTC),
                    status="success",
                    response=result.response,
                )

        routed = self._send_to_dlq(dlq, workflow_id, action, event, last_error) if dlq else False
        now = datetime.now(UTC)
        return AttemptRecord(
            action_id=action.id,
            attempt=attempt,
            started_at=now,
            finished_at=now,
            status="failed",
            error=last_error,
            dlq_routed=routed,
        )

    @staticmethod
    def _send_to_dlq(
        dlq: DLQConfig,
        workflow_id: str,
        action: ActionSpec,
        event: EventEnvelope,
        error: str | None,
    ) -> bool:
        """Write the failed event to the DLQ. Return True if it was persisted."""
        record: dict[str, Any] = {
            "dlq_enqueued_at": datetime.now(UTC).isoformat(),
            "workflow_id": workflow_id,
            "action_id": action.id,
            "event": {
                "event_id": event.event_id,
                "source": event.source,
                "type": event.type,
            },
        }
        if dlq.include_payload:
            record["event"]["payload"] = event.payload
        if dlq.include_error and error:
            record["error"] = error
        try:
            location = dlq_store.write(dlq, record)
        except (OSError, ValueError, sqlite3.Error) as exc:
            # Losing a dead letter is the worst outcome here: make it loud.
            log.error("DLQ write FAILED for event %s: %s", event.event_id, exc)
            return False
        log.info("DLQ: event %s written to %s", event.event_id, location)
        return True
