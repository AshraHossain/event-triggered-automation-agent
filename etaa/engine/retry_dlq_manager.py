"""Retry and DLQ manager — applies backoff policies and routes exhausted retries to dead-letter storage."""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from typing import Any

from ..models import AttemptRecord, ActionSpec, DLQConfig, EventEnvelope, RetryPolicy
from ..observability.logger import setup_logger

log = setup_logger(__name__)


class RetryDLQManager:
    """Orchestrates retries with backoff and final DLQ routing.

    Example:
        >>> mgr = RetryDLQManager()
        >>> record = mgr.run_with_retry(action_spec, event, executor_fn, retry_policy)
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
        """Execute *action* up to *policy.max_attempts* with backoff; route to DLQ on final failure.

        Returns the final AttemptRecord.
        """
        last_result = None
        last_error: str | None = None

        for attempt in range(1, policy.max_attempts + 1):
            try:
                result = await executor(action, event)
                return AttemptRecord(
                    action_id=action.id,
                    attempt=attempt,
                    started_at=result.executed_at,
                    finished_at=datetime.utcnow(),
                    status="success",
                    response=result.response,
                )
            except Exception as exc:
                last_error = str(exc)
                last_result = None
                error_code = getattr(exc, "error_code", None)

                if not policy.should_retry(error_code, attempt):
                    log.warning(
                        "Action %s failed (non-retryable code=%s) on attempt %d",
                        action.id, error_code, attempt,
                    )
                    # Exhausted retries — route to DLQ if configured
                    if dlq:
                        await self._send_to_dlq(dlq, action, event, last_error)
                    return AttemptRecord(
                        action_id=action.id,
                        attempt=attempt,
                        started_at=datetime.utcnow(),
                        finished_at=datetime.utcnow(),
                        status="failed",
                        error=last_error,
                    )

                if self._async_delay:
                    delay = policy.compute_delay(attempt)
                    log.info("Retrying action %s in %.2fs (attempt %d/%d)", action.id, delay, attempt, policy.max_attempts)
                    await asyncio.sleep(delay)
                else:
                    # Synchronous delay for non-async callers
                    import time
                    time.sleep(policy.compute_delay(attempt))

        # All attempts exhausted — route to DLQ
        if dlq:
            await self._send_to_dlq(dlq, action, event, last_error)

        return AttemptRecord(
            action_id=action.id,
            attempt=policy.max_attempts,
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            status="failed",
            error=last_error,
        )

    @staticmethod
    async def _send_to_dlq(dlq: DLQConfig, action: ActionSpec, event: EventEnvelope, error: str | None) -> None:
        """Write the failed payload to the configured DLQ target."""
        record: dict[str, Any] = {
            "dlq_enqueued_at": datetime.utcnow().isoformat(),
            "action_id": action.id,
            "event": {
                "event_id": event.event_id,
                "source": event.source,
                "type": event.type,
                "payload": event.payload,
            },
        }
        if dlq.include_error and error:
            record["error"] = error

        target_type = dlq.target_type.value
        target_config = dlq.target_config

        if target_type == "file":
            path = target_config.get("path", "/tmp/etaa_dlq.jsonl")
            import json
            from pathlib import Path
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, default=str) + "\n")
            log.info("DLQ: wrote to file %s", path)

        elif target_type == "queue":
            # TODO: integrate real queue client (SQS, RabbitMQ, …)
            log.warning("DLQ: queue target '%s' not yet implemented", target_config.get("queue_name", "unknown"))

        elif target_type == "db":
            # TODO: insert into database table
            log.warning("DLQ: db target not yet implemented")

        else:
            log.error("DLQ: unknown target_type=%s", target_type)
