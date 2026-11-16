"""Retry and DLQ manager.

Applies backoff policies and routes exhausted retries to dead-letter storage.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..models import ActionSpec, AttemptRecord, DLQConfig, EventEnvelope, RetryPolicy
from ..observability.logger import setup_logger

log = setup_logger(__name__)

DEFAULT_DLQ_PATH = "./data/dlq/default.jsonl"


class RetryDLQManager:
    """Orchestrates retries with backoff and final DLQ routing.

    Example:
        >>> mgr = RetryDLQManager()
        >>> record = await mgr.run_with_retry(action, event, executor_fn, policy)
    """

    def __init__(self, async_delay: bool = True) -> None:
        self._async_delay = async_delay

    async def run_with_retry(
        self,
        action: ActionSpec,
        event: EventEnvelope,
        executor,  # callable returning Awaitable[ActionResult]
        policy: RetryPolicy,
        dlq: DLQConfig | None = None,
    ) -> AttemptRecord:
        """Execute *action* up to ``policy.max_attempts`` with backoff.

        Routes to the DLQ on final failure and returns the final AttemptRecord.
        """
        last_error: str | None = None

        for attempt in range(1, policy.max_attempts + 1):
            try:
                result = await executor(action, event)
                return AttemptRecord(
                    action_id=action.id,
                    attempt=attempt,
                    started_at=result.executed_at,
                    finished_at=datetime.now(UTC),
                    status="success",
                    response=result.response,
                )
            except Exception as exc:
                last_error = str(exc)
                error_code = getattr(exc, "error_code", None)

                if not policy.should_retry(error_code, attempt):
                    log.warning(
                        "Action %s failed (non-retryable code=%s) on attempt %d",
                        action.id, error_code, attempt,
                    )
                    if dlq:
                        await self._send_to_dlq(dlq, action, event, last_error)
                    return self._failed_record(action.id, attempt, last_error)

                delay = policy.compute_delay(attempt)
                log.info(
                    "Retrying action %s in %.2fs (attempt %d/%d)",
                    action.id, delay, attempt, policy.max_attempts,
                )
                if self._async_delay:
                    await asyncio.sleep(delay)
                else:
                    time.sleep(delay)

        # All attempts exhausted — route to DLQ.
        if dlq:
            await self._send_to_dlq(dlq, action, event, last_error)
        return self._failed_record(action.id, policy.max_attempts, last_error)

    @staticmethod
    def _failed_record(action_id: str, attempt: int, error: str | None) -> AttemptRecord:
        now = datetime.now(UTC)
        return AttemptRecord(
            action_id=action_id,
            attempt=attempt,
            started_at=now,
            finished_at=now,
            status="failed",
            error=error,
        )

    @staticmethod
    async def _send_to_dlq(
        dlq: DLQConfig, action: ActionSpec, event: EventEnvelope, error: str | None
    ) -> None:
        """Write the failed event to the configured DLQ target."""
        record: dict[str, Any] = {
            "dlq_enqueued_at": datetime.now(UTC).isoformat(),
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

        target_type = dlq.target_type.value
        target_config = dlq.target_config

        if target_type == "file":
            path = Path(target_config.get("path", DEFAULT_DLQ_PATH))
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, default=str) + "\n")
            log.info("DLQ: wrote to file %s", path)
        elif target_type == "queue":
            # TODO: integrate a real queue client (SQS, RabbitMQ, …).
            log.warning(
                "DLQ: queue target '%s' not yet implemented",
                target_config.get("queue_name", "unknown"),
            )
        elif target_type == "db":
            # TODO: insert into a database table.
            log.warning("DLQ: db target not yet implemented")
        else:
            log.error("DLQ: unknown target_type=%s", target_type)
