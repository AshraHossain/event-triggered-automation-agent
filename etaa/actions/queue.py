"""Queue action — publish the event to a named queue."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any

from ..models import ActionSpec, EventEnvelope
from ..queues import SqliteQueue
from .base import ActionError, ActionResult, BaseAction, Steps

DEFAULT_QUEUE_DB = "./data/queues.db"


class QueueAction(BaseAction):
    """Publish a message to a queue.

    ``config.queue_name`` selects the queue. ``config.backend`` selects the
    implementation:

    - ``"memory"`` (default): in-process list, lost on exit.
    - ``"sqlite"``: durable queue in ``config.db_path`` (default
      ``./data/queues.db``), consumed with ``etaa consume``.
    - ``"sqs"`` / ``"rabbitmq"``: not implemented — fail loudly rather than drop.
    """

    def __init__(self) -> None:
        self._queues: dict[str, list[dict[str, Any]]] = {}

    async def execute(
        self, action: ActionSpec, event: EventEnvelope, steps: Steps | None = None
    ) -> ActionResult:
        cfg = action.config
        queue_name = cfg.get("queue_name", "default")
        backend = cfg.get("backend", "memory")
        if backend not in ("memory", "sqlite"):
            known = backend in ("sqs", "rabbitmq")
            raise ActionError(
                (
                    f"{backend} queue backend not yet implemented"
                    if known
                    else f"Unknown queue backend: {backend}"
                ),
                error_code="not_implemented" if known else "invalid_config",
                retryable=False,
            )

        message = {
            "id": event.event_id,
            "source": event.source,
            "type": event.type,
            "payload": event.payload,
            "acted_by": action.id,
            "enqueued_at": datetime.now(UTC).isoformat(),
        }
        if backend == "sqlite":
            try:
                queue = SqliteQueue(cfg.get("db_path", DEFAULT_QUEUE_DB), queue_name)
                try:
                    queue.publish(message)
                finally:
                    queue.close()
            except (OSError, sqlite3.Error) as exc:
                raise ActionError(
                    f"Queue publish failed: {exc}", error_code="queue_error"
                ) from exc
        else:
            self._queues.setdefault(queue_name, []).append(message)
        return ActionResult(
            action_id=action.id,
            success=True,
            response={"queue": queue_name, "backend": backend, "msg_id": message["id"]},
        )

    def get_queue(self, name: str) -> list[dict[str, Any]]:
        return list(self._queues.get(name, []))
