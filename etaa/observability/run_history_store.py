"""Run history store — persists WorkflowRun records for audit and replay."""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from ..models import RunRecord
from ..observability.logger import setup_logger

log = setup_logger(__name__)


class RunHistoryStore:
    """Persist and retrieve workflow execution records.

    Backing storage defaults to a JSONL file (*path/runs.jsonl*) or SQLite when
    *db_path* is provided.  JSONL is the simpler option and good for demo usage.

    Example:
        >>> store = RunHistoryStore(path="./data/runs")
        >>> store.save(record)
        >>> runs = store.list(workflow_id="pay-wf", limit=10)
    """

    def __init__(self, path: str = "./data/runs", db_path: str | None = None) -> None:
        self._db_path = db_path
        self._jsonl_path = Path(path) / "runs.jsonl"
        self._lock = threading.Lock()
        if db_path:
            self._connect_db(db_path)
        else:
            self._jsonl_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect_db(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id      TEXT PRIMARY KEY,
                event_id    TEXT,
                workflow_id TEXT,
                status      TEXT,
                attempts    INTEGER,
                started_at  TEXT,
                finished_at TEXT,
                metrics     TEXT,
                action_logs TEXT
            )
        """)
        self._conn.commit()

    def save(self, record: RunRecord) -> None:
        """Persist a completed RunRecord."""
        row = {
            "run_id": record.run_id,
            "event_id": record.event_id,
            "workflow_id": record.workflow_id,
            "status": record.status.value,
            "attempts": record.attempts,
            "started_at": record.started_at.isoformat(),
            "finished_at": record.finished_at.isoformat() if record.finished_at else None,
            "metrics": json.dumps(record.metrics, default=str),
            "action_logs": json.dumps([r.model_dump() for r in record.action_logs], default=str),
        }
        with self._lock:
            if self._db_path:
                self._save_to_db(row)
            else:
                self._save_to_jsonl(row)

    def _save_to_db(self, row: dict[str, Any]) -> None:
        try:
            self._conn.execute(
                """INSERT OR REPLACE INTO runs
                   (run_id, event_id, workflow_id, status, attempts,
                    started_at, finished_at, metrics, action_logs)
                   VALUES (:run_id, :event_id, :workflow_id, :status, :attempts,
                           :started_at, :finished_at, :metrics, :action_logs)""",
                row,
            )
            self._conn.commit()
        except Exception as exc:
            log.error("DB save failed", exc_info=exc)

    def _save_to_jsonl(self, row: dict[str, Any]) -> None:
        try:
            with open(self._jsonl_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, default=str) + "\n")
        except Exception as exc:
            log.error("JSONL save failed", exc_info=exc)

    def get(self, run_id: str) -> RunRecord | None:
        """Retrieve a single run by ID."""
        with self._lock:
            if self._db_path:
                cur = self._conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,))
                row = cur.fetchone()
                if row:
                    return self._row_to_record(dict(row))
            else:
                matched = None
                for line in self._jsonl_path.read_text(encoding="utf-8").splitlines():
                    if not line.strip():
                        continue
                    rec = json.loads(line)
                    if rec.get("run_id") == run_id:
                        matched = self._row_to_record(rec)  # keep last (latest save)
                return matched
        return None

    def list(self, workflow_id: str | None = None, limit: int = 50) -> list[RunRecord]:
        """List recent runs, optionally filtered by *workflow_id*."""
        with self._lock:
            if self._db_path:
                sql = "SELECT * FROM runs"
                params: tuple = ()
                if workflow_id:
                    sql += " WHERE workflow_id = ?"
                    params = (workflow_id,)
                sql += f" ORDER BY started_at DESC LIMIT {limit}"
                cur = self._conn.execute(sql, params)
                return [self._row_to_record(dict(r)) for r in cur.fetchall()]
            else:
                results: list[RunRecord] = []
                for line in reversed(self._jsonl_path.read_text(encoding="utf-8").splitlines()):
                    if not line.strip():
                        continue
                    rec = json.loads(line)
                    if workflow_id and rec.get("workflow_id") != workflow_id:
                        continue
                    results.append(self._row_to_record(rec))
                    if len(results) >= limit:
                        break
                return results

    @staticmethod
    def _row_to_record(row: dict[str, Any]) -> RunRecord:
        from ..models import AttemptRecord, ExecutionStatus
        finished = row.get("finished_at")
        raw_logs = json.loads(row["action_logs"]) if row.get("action_logs") else []
        return RunRecord(
            run_id=row["run_id"],
            event_id=row["event_id"],
            workflow_id=row["workflow_id"],
            status=ExecutionStatus(row["status"]),
            attempts=row["attempts"],
            started_at=datetime.fromisoformat(row["started_at"]),
            finished_at=datetime.fromisoformat(finished) if finished else None,
            metrics=json.loads(row["metrics"]) if row.get("metrics") else {},
            action_logs=[AttemptRecord(**a) for a in raw_logs],
        )

    def close(self) -> None:
        if hasattr(self, "_conn") and self._conn:
            self._conn.close()


# Singleton helper
_default_store: RunHistoryStore | None = None


def get_run_history(path: str = "./data/runs", db_path: str | None = None) -> RunHistoryStore:
    global _default_store
    if _default_store is None:
        _default_store = RunHistoryStore(path=path, db_path=db_path)
    return _default_store
