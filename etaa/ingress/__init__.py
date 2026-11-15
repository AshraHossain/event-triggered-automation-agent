"""Ingress layer — converts incoming webhooks and queue messages into EventEnvelopes."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator

from ..models import EventEnvelope
from ..observability.logger import setup_logger

log = setup_logger(__name__)


class BaseIngress(ABC):
    """Abstract base for all ingress adapters."""

    @abstractmethod
    async def ingest(self, raw: dict | bytes | Any) -> EventEnvelope:
        """Convert raw input into a normalised EventEnvelope."""
        ...


class WebhookIngress(BaseIngress):
    """Parse HTTP webhook payloads (Stripe, GitHub, custom) into EventEnvelope.

    Expected JSON shape (flexible):
    {
        "id": "evt_xxx",
        "source": "stripe",
        "type": "payment_succeeded",
        "data": { ... }   ← becomes payload
    }

    Fields ``source`` and ``type`` can also be supplied via headers:
      X-Event-Source, X-Event-Type
    """

    def __init__(self, default_source: str = "webhook", default_type: str = "generic") -> None:
        self._default_source = default_source
        self._default_type = default_type

    async def ingest(self, raw: dict | bytes | Any) -> EventEnvelope:
        if isinstance(raw, bytes):
            import json
            data = json.loads(raw)
        elif isinstance(raw, dict):
            data = raw
        else:
            data = {"raw": str(raw)}

        event_id = data.get("id") or data.get("event_id") or str(hash(str(data)) & 0xFFFFFFFF)
        source = data.get("source") or data.get("x-event-source", self._default_source)
        etype = data.get("type") or data.get("x-event-type", self._default_type)
        payload = data.get("data") or data.get("payload") or {k: v for k, v in data.items() if k not in ("id", "source", "type", "event_id")}

        return EventEnvelope(
            event_id=str(event_id),
            source=str(source),
            type=str(etype),
            payload=payload if isinstance(payload, dict) else {},
            metadata={"headers": {}},
        )


class QueueIngress(BaseIngress):
    """Ingest events from a message queue adapter.

    Each concrete queue (SQS, RabbitMQ, Kafka, in-memory) provides its own
    implementation.  The stub below uses an in-memory list for demo purposes.
    """

    def __init__(self, queue_name: str = "default") -> None:
        self._queue_name = queue_name
        self._messages: list[dict] = []

    def enqueue(self, message: dict) -> None:
        """Push a raw message into the in-memory queue (demo only)."""
        self._messages.append(message)

    async def consume_one(self) -> EventEnvelope | None:
        """Return the next message as EventEnvelope, or None if empty."""
        if not self._messages:
            return None
        raw = self._messages.pop(0)
        return await WebhookIngress().ingest(raw)

    async def consume_many(self) -> AsyncIterator[EventEnvelope]:
        """Yield events one by one until the queue is drained."""
        while self._messages:
            evt = await self.consume_one()
            if evt:
                yield evt

    @property
    def depth(self) -> int:
        return len(self._messages)


# Factory helpers
def make_webhook_ingress(**kwargs) -> WebhookIngress:
    return WebhookIngress(**kwargs)


def make_queue_ingress(queue_name: str = "default") -> QueueIngress:
    return QueueIngress(queue_name=queue_name)
