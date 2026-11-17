"""Tests for the durable SQLite queue, the queue action backend, and `etaa consume`."""

from __future__ import annotations

import json

import pytest

from etaa.actions import ActionError, QueueAction
from etaa.main import consume_queue, main, queue_dead
from etaa.models import ActionKind, ActionSpec, EventEnvelope, WorkflowSpec, WorkflowTrigger
from etaa.queues import SqliteQueue

pytestmark = pytest.mark.integration


@pytest.fixture
def queue(tmp_path):
    q = SqliteQueue(str(tmp_path / "q.db"), "jobs", visibility_timeout=0.05, max_deliveries=2)
    yield q
    q.close()


def test_publish_claim_ack_roundtrip(queue):
    first = queue.publish({"id": "a"})
    queue.publish({"id": "b"})
    assert queue.depth() == 2
    (claimed,) = queue.claim(limit=1)
    assert claimed == (first, {"id": "a"})  # oldest first
    assert [m["id"] for _, m in queue.claim(limit=5)] == ["b"]  # "a" is in flight, not visible
    queue.ack(first)
    assert queue.depth() == 1


def test_unacked_message_is_redelivered_then_becomes_dead(queue):
    import time

    queue.publish({"id": "poison"})
    for _ in range(2):  # max_deliveries = 2
        assert [m["id"] for _, m in queue.claim()] == ["poison"]
        time.sleep(0.06)  # claim expires with no ack
    assert queue.claim() == []
    assert queue.depth() == 0
    assert queue.dead_letters() == [{"id": "poison"}]


def test_queues_sharing_a_file_are_independent(tmp_path):
    db = str(tmp_path / "q.db")
    a, b = SqliteQueue(db, "a"), SqliteQueue(db, "b")
    a.publish({"id": "only-a"})
    assert b.claim() == [] and len(a.claim()) == 1
    a.close()
    b.close()


async def test_queue_action_sqlite_backend_is_durable(tmp_path):
    db = str(tmp_path / "q.db")
    action = ActionSpec(
        id="q",
        type=ActionKind.QUEUE,
        config={"backend": "sqlite", "db_path": db, "queue_name": "out"},
    )
    event = EventEnvelope(event_id="e1", source="s", type="t", payload={"k": 1})
    result = await QueueAction().execute(action, event)
    assert result.response["backend"] == "sqlite"
    reader = SqliteQueue(db, "out")
    ((_, message),) = reader.claim()
    assert message["id"] == "e1" and message["payload"] == {"k": 1}
    reader.close()


async def test_queue_action_unknown_backend_and_sqlite_error(tmp_path):
    event = EventEnvelope(event_id="e1", source="s", type="t", payload={})
    bad = ActionSpec(id="q", type=ActionKind.QUEUE, config={"backend": "kafka"})
    with pytest.raises(ActionError) as exc:
        await QueueAction().execute(bad, event)
    assert exc.value.error_code == "invalid_config"
    blocked = tmp_path / "file"
    blocked.write_text("not a directory")
    unwritable = ActionSpec(
        id="q",
        type=ActionKind.QUEUE,
        config={"backend": "sqlite", "db_path": str(blocked / "x.db")},
    )
    with pytest.raises(ActionError) as exc:
        await QueueAction().execute(unwritable, event)
    assert exc.value.error_code == "queue_error" and exc.value.retryable is True


async def test_consume_runs_workflows_and_acks(make_executor, queue):
    executor, router, registry = make_executor()
    router.add_workflow(
        WorkflowSpec(
            id="wf",
            name="wf",
            triggers=[WorkflowTrigger(source="s", type="t")],
            actions=[ActionSpec(id="out", type=ActionKind.QUEUE, config={"queue_name": "done"})],
        )
    )
    for i in range(3):
        queue.publish({"id": f"m{i}", "source": "s", "type": "t"})
    counts = await consume_queue(executor, queue, once=True)
    assert counts == {"processed": 3, "failed": 0}
    assert queue.depth() == 0
    assert len(registry["queue"].get_queue("done")) == 3


async def test_consume_keeps_the_message_when_processing_crashes(
    make_executor, queue, monkeypatch
):
    executor, _, _ = make_executor()

    async def boom(event):
        raise RuntimeError("worker died")

    monkeypatch.setattr(executor, "execute", boom)
    queue.publish({"id": "m1", "source": "s", "type": "t"})
    counts = await consume_queue(executor, queue, once=True)
    assert counts == {"processed": 0, "failed": 1}
    assert queue.depth() == 1  # not acked: it will be redelivered


def test_cli_consume_once_dry_run(tmp_path, monkeypatch, capsys):
    (tmp_path / "wf.yaml").write_text(
        "id: http-wf\nname: x\ntriggers:\n  - {source: s, type: t}\nactions:\n"
        "  - id: call\n    type: http\n    config: {method: POST, url: 'https://api.example.com/x'}\n"
    )
    db = str(tmp_path / "q.db")
    q = SqliteQueue(db, "jobs")
    q.publish({"id": "cli-1", "source": "s", "type": "t"})
    q.close()
    argv = [
        "etaa",
        "consume",
        "jobs",
        "--db",
        db,
        "--workflows",
        str(tmp_path),
        "--once",
        "--dry-run",
    ]
    monkeypatch.setattr("sys.argv", argv)
    main()
    assert json.loads(capsys.readouterr().out) == {"processed": 1, "failed": 0, "dead": 0}
    assert queue_dead(db, "jobs") == []
