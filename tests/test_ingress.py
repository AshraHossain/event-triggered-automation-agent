"""Tests for ingress normalisation and the in-memory queue adapter."""

from __future__ import annotations

import pytest

from etaa.ingress import QueueIngress, WebhookIngress

pytestmark = pytest.mark.unit


async def test_event_id_is_deterministic_without_id():
    """A payload with no id must yield the same, stable event_id every time."""
    ingress = WebhookIngress()
    data = {"source": "stripe", "type": "evt", "data": {"amount": 1}}
    first = await ingress.ingest(dict(data))
    second = await ingress.ingest(dict(data))
    assert first.event_id == second.event_id
    assert first.event_id.startswith("evt_")


async def test_ingest_accepts_bytes():
    ingress = WebhookIngress()
    event = await ingress.ingest(b'{"id": "e1", "source": "s", "type": "t"}')
    assert event.event_id == "e1"
    assert event.source == "s"


async def test_ingest_infers_payload_from_remaining_keys():
    ingress = WebhookIngress()
    event = await ingress.ingest({"id": "e1", "source": "s", "type": "t", "amount": 5})
    assert event.payload == {"amount": 5}


async def test_queue_ingress_roundtrip():
    q = QueueIngress()
    assert q.depth == 0
    q.enqueue({"id": "e1", "source": "s", "type": "t"})
    q.enqueue({"id": "e2", "source": "s", "type": "t"})
    assert q.depth == 2
    first = await q.consume_one()
    assert first.event_id == "e1"
    drained = [e.event_id async for e in q.consume_many()]
    assert drained == ["e2"]
    assert await q.consume_one() is None
