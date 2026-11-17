"""Idempotency manager — prevents duplicate workflow executions for the same event."""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from pathlib import Path

DEFAULT_TTL_SECONDS = 3600


class IdempotencyManager:
    """Track which (workflow_id, event_id) pairs have been claimed.

    A key is claimed before a run starts (so concurrent duplicates are skipped)
    and released if the run fails (so a redelivery or DLQ replay can retry it).

    Storage is in-memory by default. Pass ``db_path`` for a SQLite-backed store
    that survives restarts and is shared by processes on the same host.

    Example:
        >>> mgr = IdempotencyManager(ttl_seconds=3600)
        >>> mgr.check_and_set("pay-wf", "evt-abc")   # first time
        False
        >>> mgr.check_and_set("pay-wf", "evt-abc")   # duplicate — skip
        True
    """

    def __init__(self, ttl_seconds: int = DEFAULT_TTL_SECONDS, db_path: str | None = None) -> None:
        self._ttl = ttl_seconds
        self._lock = threading.Lock()
        self._seen: dict[str, float] = {}
        self._conn: sqlite3.Connection | None = None
        if db_path:
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(db_path, check_same_thread=False)
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS idempotency (key TEXT PRIMARY KEY, ts REAL NOT NULL)"
            )
            self._conn.commit()

    @staticmethod
    def _key(workflow_id: str, event_id: str) -> str:
        return f"{workflow_id}:{event_id}"

    def check_and_set(self, workflow_id: str, event_id: str) -> bool:
        """Claim the pair. Return True if it was ALREADY claimed (duplicate)."""
        key = self._key(workflow_id, event_id)
        now = time.time()
        cutoff = now - self._ttl
        with self._lock:
            if self._conn is not None:
                self._conn.execute("DELETE FROM idempotency WHERE ts < ?", (cutoff,))
                # The PRIMARY KEY makes the claim atomic across processes.
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO idempotency (key, ts) VALUES (?, ?)", (key, now)
                )
                self._conn.commit()
                return cur.rowcount == 0
            self._seen = {k: ts for k, ts in self._seen.items() if ts >= cutoff}
            if key in self._seen:
                return True
            self._seen[key] = now
            return False

    def release(self, workflow_id: str, event_id: str) -> None:
        """Forget the pair so the event can be processed again (after a failure)."""
        key = self._key(workflow_id, event_id)
        with self._lock:
            if self._conn is not None:
                self._conn.execute("DELETE FROM idempotency WHERE key = ?", (key,))
                self._conn.commit()
            else:
                self._seen.pop(key, None)

    def clear(self) -> None:
        """Remove all recorded ids (useful for tests)."""
        with self._lock:
            if self._conn is not None:
                self._conn.execute("DELETE FROM idempotency")
                self._conn.commit()
            self._seen.clear()

    @property
    def count(self) -> int:
        with self._lock:
            if self._conn is not None:
                return self._conn.execute("SELECT COUNT(*) FROM idempotency").fetchone()[0]
            return len(self._seen)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


_default_manager: IdempotencyManager | None = None


def get_idempotency_manager(ttl_seconds: int = DEFAULT_TTL_SECONDS) -> IdempotencyManager:
    """Return the process-wide manager; ``ETAA_STATE_DB`` makes it persistent."""
    global _default_manager
    if _default_manager is None:
        _default_manager = IdempotencyManager(
            ttl_seconds=ttl_seconds, db_path=os.environ.get("ETAA_STATE_DB")
        )
    return _default_manager
