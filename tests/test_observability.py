"""Tests for the SQLite-backed metrics and run-history stores."""

from __future__ import annotations

import pytest

from etaa.models import ExecutionStatus, RunRecord
from etaa.observability.metrics_store import MetricsStore
from etaa.observability.run_history_store import RunHistoryStore

pytestmark = pytest.mark.integration


def test_metrics_store_sqlite_roundtrip(tmp_path):
    store = MetricsStore(persist_path=str(tmp_path / "metrics.db"))
    try:
        store.increment("wf", "success_count")
        store.increment("wf", "success_count")
        store.record_latency("wf", 0.5)
        snap = store.snapshot()
        assert snap["wf"]["success_count"] == 2
        assert snap["wf"]["latency_samples"] == 1
        assert snap["wf"]["latency_avg"] == 0.5
    finally:
        store.close()


def test_run_history_sqlite_roundtrip(tmp_path):
    store = RunHistoryStore(db_path=str(tmp_path / "runs.db"))
    try:
        rec = RunRecord(
            run_id="r1", event_id="e1", workflow_id="wf", status=ExecutionStatus.RUNNING
        )
        rec.finish(ExecutionStatus.SUCCESS)
        store.save(rec)

        fetched = store.get("r1")
        assert fetched is not None
        assert fetched.status == ExecutionStatus.SUCCESS

        listed = store.list(workflow_id="wf", limit=10)
        assert [r.run_id for r in listed] == ["r1"]
        assert store.list(workflow_id="other") == []
    finally:
        store.close()
