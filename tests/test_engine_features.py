"""Tests for step outputs, retry classification, idempotency release, DLQ, and replay."""

from __future__ import annotations

import json
import sys
import types

import httpx
import pytest

from etaa.engine import dlq as dlq_store
from etaa.engine.idempotency_manager import IdempotencyManager
from etaa.main import replay_dlq
from etaa.models import (
    ActionKind,
    ActionSpec,
    DLQConfig,
    DLQTargetKind,
    EventEnvelope,
    ExecutionStatus,
    RetryPolicy,
    WorkflowSpec,
    WorkflowTrigger,
)

pytestmark = pytest.mark.integration


def _transport(status: int, calls: list[httpx.Request] | None = None) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if calls is not None:
            calls.append(request)
        return httpx.Response(status, json={"ok": status == 200})

    return httpx.MockTransport(handler)


def _http_wf(dlq: DLQConfig, max_attempts: int = 2) -> WorkflowSpec:
    return WorkflowSpec(
        id="wf",
        name="wf",
        triggers=[WorkflowTrigger(source="s", type="t")],
        actions=[
            ActionSpec(
                id="call",
                type=ActionKind.HTTP,
                config={"method": "POST", "url": "https://api.example.com/x", "body": {"a": 1}},
            )
        ],
        retry_policy=RetryPolicy(max_attempts=max_attempts),
        dlq=dlq,
    )


def _event(event_id: str = "e1") -> EventEnvelope:
    return EventEnvelope(event_id=event_id, source="s", type="t", payload={"k": "v"})


async def test_step_outputs_flow_to_later_actions(make_executor):
    """An AI classification is visible to a function handler and an HTTP body."""
    calls: list[httpx.Request] = []
    executor, router, _ = make_executor(_transport(200, calls))
    seen: dict = {}
    mod = types.ModuleType("examples._steps_probe")
    mod.probe = lambda event, context: seen.update(context["steps"]) or {"owner": "bob"}
    sys.modules["examples._steps_probe"] = mod
    router.add_workflow(
        WorkflowSpec(
            id="wf",
            name="wf",
            triggers=[WorkflowTrigger(source="s", type="t")],
            actions=[
                ActionSpec(
                    id="classify",
                    type=ActionKind.AI,
                    config={"provider": "stub", "labels": ["billing"], "default_label": "billing"},
                ),
                ActionSpec(
                    id="assign",
                    type=ActionKind.FUNCTION,
                    config={"module_path": "examples._steps_probe.probe"},
                    depends_on=["classify"],
                ),
                ActionSpec(
                    id="notify",
                    type=ActionKind.HTTP,
                    config={
                        "method": "POST",
                        "url": "https://api.example.com/n",
                        "body": {
                            "category": "{{steps.classify.classification}}",
                            "owner": "{{steps.assign.result.owner}}",
                        },
                    },
                    depends_on=["assign"],
                ),
            ],
        )
    )
    try:
        (run,) = await executor.execute(_event())
    finally:
        sys.modules.pop("examples._steps_probe", None)
    assert run.status == ExecutionStatus.SUCCESS
    assert seen["classify"]["classification"] == "billing"
    assert json.loads(calls[0].content) == {"category": "billing", "owner": "bob"}


async def test_failed_run_releases_idempotency_and_can_be_retried(make_executor, tmp_path):
    dlq = DLQConfig(target_config={"path": str(tmp_path / "dlq.jsonl")})
    failing, router, _ = make_executor(_transport(500))
    router.add_workflow(_http_wf(dlq))
    (first,) = await failing.execute(_event())
    assert first.status == ExecutionStatus.FAILED
    assert first.action_logs[0].dlq_routed is True
    assert failing.metrics.snapshot()["wf"]["dlq_count"] == 1
    assert failing.metrics.snapshot()["wf"]["retry_count"] == 1
    # Redelivery of the same event is executed again, not SKIPPED.
    (second,) = await failing.execute(_event())
    assert second.status == ExecutionStatus.FAILED


async def test_non_retryable_error_is_not_retried(make_executor, tmp_path):
    executor, router, _ = make_executor()
    wf = _http_wf(DLQConfig(target_config={"path": str(tmp_path / "d.jsonl")}), max_attempts=5)
    wf.actions[0].config["url"] = "http://127.0.0.1/admin"  # blocked by the SSRF guard
    router.add_workflow(wf)
    (run,) = await executor.execute(_event())
    assert run.status == ExecutionStatus.FAILED
    assert run.attempts == 1
    assert "private or loopback" in run.action_logs[0].error


def _spec(actions: list[ActionSpec]) -> WorkflowSpec:
    return WorkflowSpec(
        id="wf", name="wf", triggers=[WorkflowTrigger(source="s", type="t")], actions=actions
    )


def test_workflow_graph_is_validated_at_load_time():
    with pytest.raises(ValueError, match="dependency cycle"):
        _spec(
            [
                ActionSpec(id="a", type=ActionKind.QUEUE, depends_on=["b"]),
                ActionSpec(id="b", type=ActionKind.QUEUE, depends_on=["a"]),
            ]
        )
    with pytest.raises(ValueError, match="unknown actions"):
        _spec([ActionSpec(id="a", type=ActionKind.QUEUE, depends_on=["ghost"])])
    with pytest.raises(ValueError, match="duplicate action ids"):
        _spec([ActionSpec(id="a", type=ActionKind.QUEUE), ActionSpec(id="a", type=ActionKind.AI)])


async def test_graph_mutated_after_validation_still_fails_the_run(make_executor):
    """Defence in depth: the executor re-checks the graph it is about to run."""
    executor, router, _ = make_executor()
    wf = _spec(
        [ActionSpec(id="a", type=ActionKind.QUEUE), ActionSpec(id="b", type=ActionKind.QUEUE)]
    )
    wf.actions[0].depends_on.append("b")
    wf.actions[1].depends_on.append("a")
    router.add_workflow(wf)
    (run,) = await executor.execute(_event())
    assert run.status == ExecutionStatus.FAILED


async def test_retry_resumes_at_the_failed_action(make_executor, tmp_path):
    """A completed step is not repeated when the same event is processed again."""
    calls: list[str] = []
    healthy = {"crm": False}

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/crm" and not healthy["crm"]:
            return httpx.Response(500)
        return httpx.Response(200, json={"ok": True})

    executor, router, _ = make_executor(httpx.MockTransport(handler))

    def http(action_id: str, path: str, deps: list[str]) -> ActionSpec:
        config = {"method": "POST", "url": f"https://api.example.com{path}"}
        return ActionSpec(id=action_id, type=ActionKind.HTTP, config=config, depends_on=deps)

    wf = _spec([http("email", "/email", []), http("crm", "/crm", ["email"])])
    wf.retry_policy.max_attempts = 1
    wf.dlq.target_config["path"] = str(tmp_path / "dlq.jsonl")
    router.add_workflow(wf)

    (first,) = await executor.execute(_event())
    assert first.status == ExecutionStatus.FAILED
    healthy["crm"] = True
    (second,) = await executor.execute(_event())  # redelivery / replay

    assert second.status == ExecutionStatus.SUCCESS
    assert [log.status for log in second.action_logs] == ["resumed", "success"]
    assert calls == ["/email", "/crm", "/crm"]  # the email was sent exactly once
    # Checkpoints are cleared on success: a fresh event id runs every step.
    await executor.execute(_event("e2"))
    assert calls.count("/email") == 2


def test_action_cannot_depend_on_itself():
    with pytest.raises(ValueError, match="cannot depend on itself"):
        ActionSpec(id="a", type=ActionKind.QUEUE, depends_on=["a"])


async def test_replay_reruns_dead_letters_and_clears_them(make_executor, tmp_path):
    path = str(tmp_path / "dlq.jsonl")
    dlq = DLQConfig(target_config={"path": path})
    failing, router, _ = make_executor(_transport(500))
    router.add_workflow(_http_wf(dlq))
    await failing.execute(_event("e1"))
    await failing.execute(_event("e2"))
    assert len(dlq_store.load(path)) == 2

    healthy, router2, _ = make_executor(_transport(200))
    router2.add_workflow(_http_wf(dlq))
    counts = await replay_dlq(healthy, path)
    assert counts == {"replayed": 2, "succeeded": 2, "failed": 0, "skipped": 0}
    assert dlq_store.load(path) == []


async def test_replay_keeps_events_that_fail_again(make_executor, tmp_path):
    path = str(tmp_path / "dlq.jsonl")
    dlq = DLQConfig(target_config={"path": path})
    failing, router, _ = make_executor(_transport(500))
    router.add_workflow(_http_wf(dlq))
    await failing.execute(_event())
    counts = await replay_dlq(failing, path)
    assert counts["failed"] == 1
    assert [r["event"]["event_id"] for r in dlq_store.load(path)] == ["e1"]


async def test_replay_skips_unknown_workflow_and_payloadless_records(make_executor, tmp_path):
    path = tmp_path / "dlq.jsonl"
    rows = [
        {
            "workflow_id": "gone",
            "event": {"event_id": "e", "source": "s", "type": "t", "payload": {}},
        },
        {"workflow_id": "wf", "event": {"event_id": "e", "source": "s", "type": "t"}},
    ]
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))
    executor, router, _ = make_executor()
    router.add_workflow(_http_wf(DLQConfig()))
    counts = await replay_dlq(executor, str(path))
    assert counts["skipped"] == 2


def test_dlq_sqlite_roundtrip(tmp_path):
    db = str(tmp_path / "dlq.db")
    cfg = DLQConfig(
        target_type=DLQTargetKind.DB, target_config={"db_path": db, "table_name": "t1"}
    )
    dlq_store.write(cfg, {"n": 1})
    dlq_store.write(cfg, {"n": 2})
    assert dlq_store.load(db, "t1") == [{"n": 1}, {"n": 2}]
    dlq_store.drop_first(db, 1, "t1")
    assert dlq_store.load(db, "t1") == [{"n": 2}]


def test_dlq_rejects_unsafe_table_name(tmp_path):
    cfg = DLQConfig(
        target_type=DLQTargetKind.DB,
        target_config={"db_path": str(tmp_path / "d.db"), "table_name": "x; DROP TABLE y"},
    )
    with pytest.raises(ValueError, match="Invalid DLQ table name"):
        dlq_store.write(cfg, {})


def test_dlq_queue_target_falls_back_to_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cfg = DLQConfig(target_type=DLQTargetKind.QUEUE, target_config={"queue_name": "failed/../q"})
    location = dlq_store.write(cfg, {"n": 1})
    assert location.endswith(".jsonl") and ".." not in location.replace("._", "")
    assert dlq_store.load(location) == [{"n": 1}]


def test_idempotency_release_and_sqlite_persistence(tmp_path):
    db = str(tmp_path / "idem.db")
    first = IdempotencyManager(db_path=db)
    assert first.check_and_set("wf", "e1") is False
    first.close()
    # A new process (new instance) still sees the claim.
    second = IdempotencyManager(db_path=db)
    assert second.check_and_set("wf", "e1") is True
    second.release("wf", "e1")
    assert second.check_and_set("wf", "e1") is False
    assert second.count == 1
    second.clear()
    assert second.count == 0
    second.close()


def test_idempotency_ttl_expiry():
    mgr = IdempotencyManager(ttl_seconds=-1)  # everything is already expired
    assert mgr.check_and_set("wf", "e1") is False
    assert mgr.check_and_set("wf", "e1") is False


def test_state_stores_persist_in_sqlite(tmp_path):
    from etaa.engine.state_store import CheckpointStore, Inbox

    db = str(tmp_path / "state.db")
    writer = CheckpointStore(db)
    writer.save("wf", "e1", "email", {"status": 200})
    writer.close()
    reopened = CheckpointStore(db)  # a new process sees the completed step
    assert reopened.load("wf", "e1") == {"email": {"status": 200}}
    reopened.clear("wf", "e1")
    assert reopened.load("wf", "e1") == {}
    reopened.close()

    event = EventEnvelope(event_id="e1", source="s", type="t", payload={"k": 1})
    first = Inbox(db)
    first.put(event)
    first.put(event)  # a duplicate delivery is stored once
    first.close()
    reopened_inbox = Inbox(db)
    assert [e.event_id for e in reopened_inbox.pending()] == ["e1"]
    reopened_inbox.done(event)
    assert reopened_inbox.pending() == []
    reopened_inbox.close()


async def test_replay_counts_duplicate_dead_letters_as_skipped(make_executor, tmp_path):
    """Two dead letters for the same event: the second is already done, not a failure."""
    path = tmp_path / "dlq.jsonl"
    dlq = DLQConfig(target_config={"path": str(path)})
    record = {
        "workflow_id": "wf",
        "event": {"event_id": "e1", "source": "s", "type": "t", "payload": {}},
    }
    path.write_text((json.dumps(record) + "\n") * 2)
    executor, router, _ = make_executor(_transport(200))
    router.add_workflow(_http_wf(dlq))
    counts = await replay_dlq(executor, str(path))
    assert counts == {"replayed": 1, "succeeded": 1, "failed": 0, "skipped": 1}
