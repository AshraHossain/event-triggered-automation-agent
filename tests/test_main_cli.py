"""Tests for the application factory, workflow loading, and CLI commands."""

from __future__ import annotations

import json

import pytest

from etaa.engine.event_router import EventRouter
from etaa.main import build_application, load_workflows, main

pytestmark = pytest.mark.integration

QUEUE_WF = """
id: cli-wf
name: CLI Workflow
triggers:
  - source: test
    type: ping
actions:
  - id: enqueue
    type: queue
    config:
      queue_name: cli-q
"""


@pytest.fixture
def workflows_dir(tmp_path):
    (tmp_path / "wf.yaml").write_text(QUEUE_WF)
    return tmp_path


def test_build_application_loads_workflows(workflows_dir):
    app, router, executor = build_application(workflows_path=str(workflows_dir))
    assert router.workflow_count == 1
    assert app is not None


def test_load_workflows_from_single_file(workflows_dir):
    router = EventRouter()
    specs = load_workflows(str(workflows_dir / "wf.yaml"), router)
    assert len(specs) == 1
    assert specs[0].id == "cli-wf"


def test_load_workflows_missing_path_raises(tmp_path):
    router = EventRouter()
    with pytest.raises(FileNotFoundError):
        load_workflows(str(tmp_path / "nope"), router)


def test_cli_list(workflows_dir, monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["etaa", "list", "--workflows", str(workflows_dir)])
    main()
    out = capsys.readouterr().out
    assert "cli-wf" in out


def test_cli_metrics(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["etaa", "metrics"])
    main()
    # Snapshot is valid JSON (possibly empty object).
    json.loads(capsys.readouterr().out)


def test_cli_run_offline(workflows_dir, tmp_path, monkeypatch, capsys):
    event_file = tmp_path / "event.json"
    event_file.write_text(json.dumps({"id": "e1", "source": "test", "type": "ping"}))
    monkeypatch.setattr(
        "sys.argv",
        ["etaa", "run", str(event_file), "--workflows", str(workflows_dir)],
    )
    main()
    out = capsys.readouterr().out
    assert "cli-wf" in out
    assert "success" in out


HTTP_WF = """
id: http-wf
name: HTTP Workflow
triggers:
  - source: test
    type: call
actions:
  - id: notify
    type: http
    config:
      method: POST
      url: https://api.example.com/notify
dlq:
  target_type: file
  target_config:
    path: {dlq}
"""


def test_cli_run_dry_run_sends_nothing(tmp_path, monkeypatch, capsys):
    (tmp_path / "wf.yaml").write_text(HTTP_WF.format(dlq=tmp_path / "dlq.jsonl"))
    event_file = tmp_path / "event.json"
    event_file.write_text(json.dumps({"id": "dry-1", "source": "test", "type": "call"}))
    argv = ["etaa", "run", str(event_file), "--workflows", str(tmp_path / "wf.yaml"), "--dry-run"]
    monkeypatch.setattr("sys.argv", argv)
    main()
    out = capsys.readouterr().out
    assert '"status": "success"' in out
    assert '"dry_run": true' in out


def test_cli_run_reports_no_match(workflows_dir, tmp_path, monkeypatch, capsys):
    event_file = tmp_path / "event.json"
    event_file.write_text(json.dumps({"id": "nm-1", "source": "nobody", "type": "x"}))
    argv = ["etaa", "run", str(event_file), "--workflows", str(workflows_dir)]
    monkeypatch.setattr("sys.argv", argv)
    main()
    assert "No workflow matched" in capsys.readouterr().out


def test_cli_replay_dry_run(tmp_path, monkeypatch, capsys):
    dlq = tmp_path / "dlq.jsonl"
    (tmp_path / "wf.yaml").write_text(HTTP_WF.format(dlq=dlq))
    record = {
        "workflow_id": "http-wf",
        "action_id": "notify",
        "event": {"event_id": "rp-1", "source": "test", "type": "call", "payload": {}},
    }
    dlq.write_text(json.dumps(record) + "\n")
    argv = ["etaa", "replay", str(dlq), "--workflows", str(tmp_path / "wf.yaml"), "--dry-run"]
    monkeypatch.setattr("sys.argv", argv)
    main()
    assert json.loads(capsys.readouterr().out)["succeeded"] == 1
    assert dlq.read_text() == ""


def test_cli_validate_ok_and_invalid(workflows_dir, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["etaa", "validate", str(workflows_dir)])
    with pytest.raises(SystemExit) as ok:
        main()
    assert ok.value.code == 0
    assert "OK" in capsys.readouterr().out

    bad = tmp_path / "bad"
    bad.mkdir()
    (bad / "cycle.yaml").write_text(
        QUEUE_WF + "    depends_on: [enqueue]\n"  # an action depending on itself
    )
    (bad / "broken.yaml").write_text("id: [unclosed")
    (bad / "dup1.yaml").write_text(QUEUE_WF)
    (bad / "dup2.yaml").write_text(QUEUE_WF)
    monkeypatch.setattr("sys.argv", ["etaa", "validate", str(bad)])
    with pytest.raises(SystemExit) as failed:
        main()
    out = capsys.readouterr().out
    assert failed.value.code == 1
    assert out.count("INVALID") == 3
    assert "duplicate workflow id" in out and "cannot depend on itself" in out


def test_cli_validate_missing_path(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["etaa", "validate", str(tmp_path / "nope")])
    with pytest.raises(SystemExit) as failed:
        main()
    assert failed.value.code == 1
    assert "not found" in capsys.readouterr().out
