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
