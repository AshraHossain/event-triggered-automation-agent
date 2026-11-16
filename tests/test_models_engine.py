"""Unit tests for core models and engine logic."""

from __future__ import annotations

from datetime import datetime

import pytest

from etaa.engine.event_router import EventRouter
from etaa.engine.idempotency_manager import IdempotencyManager
from etaa.models import (
    BackoffStrategy,
    DLQConfig,
    DLQTargetKind,
    EventEnvelope,
    ExecutionStatus,
    RetryPolicy,
    RunRecord,
    WorkflowSpec,
    WorkflowTrigger,
)

# ─── EventEnvelope ───────────────────────────────────────────────────────────


def test_event_envelope_defaults():
    evt = EventEnvelope(
        event_id="e1", source="stripe", type="payment_succeeded", payload={"amount": 100}
    )
    assert evt.event_id == "e1"
    assert isinstance(evt.received_at, datetime)


def test_event_envelope_idempotency_key():
    evt = EventEnvelope(event_id="e1", source="stripe", type="payment_succeeded", payload={})
    key = evt.idempotency_key("pay-wf")
    assert key == "pay-wf:e1"


# ─── WorkflowTrigger matching ────────────────────────────────────────────────


def test_trigger_matches():
    trigger = WorkflowTrigger(source="stripe", type="payment_succeeded", filters={"success": True})
    evt = EventEnvelope(
        event_id="e1", source="stripe", type="payment_succeeded", payload={"success": True}
    )
    assert trigger.matches(evt) is True

    evt_wrong_source = EventEnvelope(
        event_id="e1", source="github", type="payment_succeeded", payload={}
    )
    assert trigger.matches(evt_wrong_source) is False

    evt_wrong_filter = EventEnvelope(
        event_id="e1", source="stripe", type="payment_succeeded", payload={"success": False}
    )
    assert trigger.matches(evt_wrong_filter) is False


def test_workflow_spec_or_semantics():
    wf = WorkflowSpec(
        id="wf1",
        name="W1",
        triggers=[
            WorkflowTrigger(source="stripe", type="payment_succeeded"),
            WorkflowTrigger(source="github", type="push"),
        ],
        actions=[],
    )
    assert (
        wf.matches(
            EventEnvelope(event_id="e1", source="stripe", type="payment_succeeded", payload={})
        )
        is True
    )
    assert (
        wf.matches(EventEnvelope(event_id="e1", source="github", type="push", payload={})) is True
    )
    assert (
        wf.matches(EventEnvelope(event_id="e1", source="slack", type="message", payload={}))
        is False
    )


# ─── RetryPolicy ──────────────────────────────────────────────────────────────


def test_fixed_backoff():
    p = RetryPolicy(max_attempts=3, backoff_strategy=BackoffStrategy.FIXED, backoff_seconds=2.0)
    assert p.compute_delay(1) == 2.0
    assert p.compute_delay(2) == 2.0


def test_exponential_backoff():
    p = RetryPolicy(
        max_attempts=5, backoff_strategy=BackoffStrategy.EXPONENTIAL, backoff_seconds=1.0
    )
    assert p.compute_delay(1) == 1.0
    assert p.compute_delay(2) == 2.0
    assert p.compute_delay(3) == 4.0
    assert p.compute_delay(10) == 60.0  # capped at 60s


def test_should_retry():
    p = RetryPolicy(max_attempts=2, retryable_error_codes=["timeout", 500])
    assert p.should_retry(None, 1) is True  # generic exception → retry
    assert p.should_retry("timeout", 1) is True
    assert p.should_retry(500, 1) is True
    assert p.should_retry(404, 1) is False
    assert p.should_retry(None, 2) is False  # max reached


# ─── IdempotencyManager ───────────────────────────────────────────────────────


def test_idempotency_detects_duplicate():
    mgr = IdempotencyManager(ttl_seconds=60)
    assert mgr.check_and_set("wf1", "evt-a") is False
    assert mgr.check_and_set("wf1", "evt-a") is True
    assert mgr.check_and_set("wf1", "evt-b") is False
    assert mgr.count == 2


def test_idempotency_different_workflow():
    mgr = IdempotencyManager()
    assert mgr.check_and_set("wf-a", "evt-x") is False
    assert mgr.check_and_set("wf-b", "evt-x") is False  # different workflow


def test_idempotency_different_event():
    mgr = IdempotencyManager()
    assert mgr.check_and_set("wf1", "evt-x") is False
    assert mgr.check_and_set("wf1", "evt-y") is False


# ─── EventRouter ──────────────────────────────────────────────────────────────


def test_router_matches_multiple():
    router = EventRouter()
    wf1 = WorkflowSpec(
        id="w1", name="W1", triggers=[WorkflowTrigger(source="s", type="t1")], actions=[]
    )
    wf2 = WorkflowSpec(
        id="w2", name="W2", triggers=[WorkflowTrigger(source="s", type="t1")], actions=[]
    )
    router.add_workflow(wf1)
    router.add_workflow(wf2)
    evt = EventEnvelope(event_id="e1", source="s", type="t1", payload={})
    matches = router.resolve(evt)
    assert len(matches) == 2
    assert {m.id for m in matches} == {"w1", "w2"}


def test_router_no_match():
    router = EventRouter()
    router.add_workflow(
        WorkflowSpec(
            id="w1", name="W1", triggers=[WorkflowTrigger(source="a", type="b")], actions=[]
        )
    )
    evt = EventEnvelope(event_id="e1", source="x", type="y", payload={})
    assert router.resolve(evt) == []


# ─── DLQConfig ────────────────────────────────────────────────────────────────


def test_dlq_defaults():
    cfg = DLQConfig()
    assert cfg.target_type == DLQTargetKind.FILE
    assert cfg.include_payload is True
    assert cfg.include_error is True


# ─── RunRecord ────────────────────────────────────────────────────────────────


def test_run_record_finish():
    rec = RunRecord(run_id="r1", event_id="e1", workflow_id="w1", status=ExecutionStatus.RUNNING)
    rec.finish(ExecutionStatus.SUCCESS)
    assert rec.status == ExecutionStatus.SUCCESS
    assert rec.finished_at is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
