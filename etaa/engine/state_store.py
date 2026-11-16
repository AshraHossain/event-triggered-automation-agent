"""Run state that lets work survive failures and restarts.

- ``CheckpointStore`` remembers which actions of a run already succeeded, so a
  redelivery or dead-letter replay resumes at the failed action instead of
  repeating completed side effects.
- ``Inbox`` holds accepted events until they finish processing, so an event
  acknowledged with 202 is not lost if the process stops mid-run.

Both are in-memory by default and SQLite-backed when given ``db_path``
(``ETAA_STATE_DB``).
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from ..models import EventEnvelope


def _connect(db_path: str, ddl: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)  # callers hold a lock
    conn.execute(ddl)
    conn.commit()
    return conn


class CheckpointStore:
    """Outputs of the actions that already succeeded for a (workflow, event) pair."""

    _DDL = (
        "CREATE TABLE IF NOT EXISTS checkpoints ("
        "run_key TEXT NOT NULL, action_id TEXT NOT NULL, response TEXT NOT NULL, "
        "PRIMARY KEY (run_key, action_id))"
    )

    def __init__(self, db_path: str | None = None) -> None:
        self._lock = threading.Lock()
        self._memory: dict[str, dict[str, Any]] = {}
        self._conn = _connect(db_path, self._DDL) if db_path else None

    @staticmethod
    def _key(workflow_id: str, event_id: str) -> str:
        return f"{workflow_id}:{event_id}"

    def load(self, workflow_id: str, event_id: str) -> dict[str, Any]:
        """Return ``{action_id: response}`` for the actions already completed."""
        key = self._key(workflow_id, event_id)
        with self._lock:
            if self._conn is None:
                return dict(self._memory.get(key, {}))
            rows = self._conn.execute(
                "SELECT action_id, response FROM checkpoints WHERE run_key = ?", (key,)
            ).fetchall()
            return {action_id: json.loads(response) for action_id, response in rows}

    def save(self, workflow_id: str, event_id: str, action_id: str, response: Any) -> None:
        key = self._key(workflow_id, event_id)
        with self._lock:
            if self._conn is None:
                self._memory.setdefault(key, {})[action_id] = response
                return
            self._conn.execute(
                "INSERT OR REPLACE INTO checkpoints (run_key, action_id, response) "
                "VALUES (?, ?, ?)",
                (key, action_id, json.dumps(response, default=str)),
            )
            self._conn.commit()

    def clear(self, workflow_id: str, event_id: str) -> None:
        """Forget a run's checkpoints (called once the run succeeds)."""
        key = self._key(workflow_id, event_id)
        with self._lock:
            if self._conn is None:
                self._memory.pop(key, None)
                return
            self._conn.execute("DELETE FROM checkpoints WHERE run_key = ?", (key,))
            self._conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class Inbox:
    """Events accepted by the server that have not finished processing yet."""

    _DDL = "CREATE TABLE IF NOT EXISTS inbox (key TEXT PRIMARY KEY, envelope TEXT NOT NULL)"

    def __init__(self, db_path: str | None = None) -> None:
        self._lock = threading.Lock()
        self._memory: dict[str, str] = {}
        self._conn = _connect(db_path, self._DDL) if db_path else None

    @staticmethod
    def _key(event: EventEnvelope) -> str:
        return f"{event.source}:{event.event_id}"

    def put(self, event: EventEnvelope) -> None:
        """Record *event* before it is acknowledged."""
        key, body = self._key(event), event.model_dump_json()
        with self._lock:
            if self._conn is None:
                self._memory.setdefault(key, body)
                return
            self._conn.execute(
                "INSERT OR IGNORE INTO inbox (key, envelope) VALUES (?, ?)", (key, body)
            )
            self._conn.commit()

    def done(self, event: EventEnvelope) -> None:
        """Remove *event* once every matching workflow has finished."""
        key = self._key(event)
        with self._lock:
            if self._conn is None:
                self._memory.pop(key, None)
                return
            self._conn.execute("DELETE FROM inbox WHERE key = ?", (key,))
            self._conn.commit()

    def pending(self) -> list[EventEnvelope]:
        """Events that were accepted but never finished (for example after a crash)."""
        with self._lock:
            if self._conn is None:
                bodies = list(self._memory.values())
            else:
                bodies = [row[0] for row in self._conn.execute("SELECT envelope FROM inbox")]
        return [EventEnvelope.model_validate_json(body) for body in bodies]

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
