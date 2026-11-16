"""Queue action — publish the event to a named queue."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from ..models import ActionSpec, EventEnvelope
from .base import ActionError, ActionResult, BaseAction, Steps


class QueueAction(BaseAction):
    """Publish a message to a queue.

    ``config.queue_name`` selects the queue. ``config.backend`` selects the
    implementation: ``"memory"`` (default, in-process) or ``"sqs"`` /
    ``"rabbitmq"`` (not implemented — fails loudly rather than dropping).
    """

    def __init__(self) -> None:
        self._queues: dict[str, list[dict[str, Any]]] = {}

    async def execute(
        self, action: ActionSpec, event: EventEnvelope, steps: Steps | None = None
    ) -> ActionResult:
        cfg = action.config
        queue_name = cfg.get("queue_name", "default")
        backend = cfg.get("backend", "memory")
        if backend != "memory":
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
        self._queues.setdefault(queue_name, []).append(message)
        return ActionResult(
            action_id=action.id,
            success=True,
            response={"queue": queue_name, "msg_id": message["id"]},
        )

    def get_queue(self, name: str) -> list[dict[str, Any]]:
        return list(self._queues.get(name, []))
