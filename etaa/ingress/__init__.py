"""Ingress layer — converts incoming webhooks and queue messages into EventEnvelopes."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any

from ..models import EventEnvelope
from ..observability.logger import setup_logger

log = setup_logger(__name__)

DEFAULT_SOURCE = "webhook"
DEFAULT_TYPE = "generic"


def resolve_source(data: dict[str, Any], default: str = DEFAULT_SOURCE) -> str:
    """Return the effective event source for *data*.

    This is the single source-of-truth used for BOTH signature-secret
    selection and event routing, so the two can never drift (which would allow
    a source-confusion auth bypass).
    """
    return str(data.get("source") or data.get("x-event-source") or default)


def _stable_event_id(data: dict[str, Any]) -> str:
    """Derive a deterministic event id from *data* when none is supplied.

    ``hash()`` is salted per process, so it cannot be used for idempotency —
    the same event would dedupe differently across workers. A content hash is
    stable across processes and restarts.
    """
    canonical = json.dumps(data, sort_keys=True, default=str)
    return "evt_" + hashlib.sha256(canonical.encode()).hexdigest()[:24]


def webhook_secret(source: str) -> str | None:
    """Return the configured HMAC secret for *source*, if any.

    Looks up ``ETAA_WEBHOOK_SECRET_<SOURCE>`` (uppercased) first, then the
    global ``ETAA_WEBHOOK_SECRET``. Returns None when signature verification
    is not configured for this source.
    """
    specific = os.environ.get(f"ETAA_WEBHOOK_SECRET_{source.upper()}")
    return specific or os.environ.get("ETAA_WEBHOOK_SECRET")


def signing_enabled() -> bool:
    """Return True if any webhook signing secret is configured.

    When signing is enabled, sources without a usable secret must be rejected
    (fail closed) rather than accepted unsigned.
    """
    return any(
        k == "ETAA_WEBHOOK_SECRET" or k.startswith("ETAA_WEBHOOK_SECRET_") for k in os.environ
    )


def unsigned_sources() -> set[str]:
    """Sources explicitly exempt from signing (``ETAA_WEBHOOK_UNSIGNED_SOURCES``)."""
    raw = os.environ.get("ETAA_WEBHOOK_UNSIGNED_SOURCES", "")
    return {s.strip() for s in raw.split(",") if s.strip()}


def verify_signature(secret: str, raw_body: bytes, signature: str | None) -> bool:
    """Return True if *signature* is a valid ``sha256=<hex>`` HMAC of *raw_body*.

    Comparison is constant-time to avoid timing side channels.
    """
    if not signature:
        return False
    expected = hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
    provided = signature[7:] if signature.startswith("sha256=") else signature
    return hmac.compare_digest(expected, provided)


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
            data = json.loads(raw)
        elif isinstance(raw, dict):
            data = raw
        else:
            data = {"raw": str(raw)}

        event_id = data.get("id") or data.get("event_id") or _stable_event_id(data)
        source = resolve_source(data, self._default_source)
        etype = data.get("type") or data.get("x-event-type") or self._default_type
        reserved = ("id", "source", "type", "event_id")
        payload = (
            data.get("data")
            or data.get("payload")
            or {k: v for k, v in data.items() if k not in reserved}
        )

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

    async def ingest(self, raw: dict | bytes | Any) -> EventEnvelope:
        """Normalise a single raw queue message into an EventEnvelope."""
        return await WebhookIngress().ingest(raw)

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
