"""Durable local message queue backed by SQLite.

At-least-once delivery: a consumer *claims* a message, processes it, then
*acks* (deletes) it. A claim that is never acked (the consumer crashed)
becomes visible again after ``visibility_timeout`` seconds, so the message is
redelivered. Idempotency and step checkpoints make that redelivery safe.

One database file can hold many named queues. It needs no broker, so it is
suited to a single host; swap it for SQS/RabbitMQ behind the same three calls
(``publish``, ``claim``, ``ack``) when running across hosts.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

DEFAULT_VISIBILITY_TIMEOUT = 60.0
DEFAULT_MAX_DELIVERIES = 5


class SqliteQueue:
    """A named queue in a SQLite file."""

    _DDL = (
        "CREATE TABLE IF NOT EXISTS queue_messages ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, queue TEXT NOT NULL, body TEXT NOT NULL, "
        "claimed_at REAL, deliveries INTEGER NOT NULL DEFAULT 0)"
    )

    def __init__(
        self,
        db_path: str,
        name: str = "default",
        visibility_timeout: float = DEFAULT_VISIBILITY_TIMEOUT,
        max_deliveries: int = DEFAULT_MAX_DELIVERIES,
    ) -> None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.name = name
        self._visibility_timeout = visibility_timeout
        self._max_deliveries = max_deliveries
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)  # guarded by _lock
        self._conn.execute(self._DDL)
        self._conn.commit()

    def publish(self, message: dict[str, Any]) -> int:
        """Append *message* and return its id."""
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO queue_messages (queue, body) VALUES (?, ?)",
                (self.name, json.dumps(message, default=str)),
            )
            self._conn.commit()
            return int(cur.lastrowid or 0)

    def claim(self, limit: int = 1) -> list[tuple[int, dict[str, Any]]]:
        """Claim up to *limit* visible messages, oldest first.

        A message delivered ``max_deliveries`` times without an ack is a poison
        message: it is moved out of the way (``dead``) instead of looping forever.
        """
        now = time.time()
        with self._lock:
            self._conn.execute(
                "UPDATE queue_messages SET claimed_at = -1 "
                "WHERE queue = ? AND deliveries >= ? AND (claimed_at IS NULL OR claimed_at < ?)",
                (self.name, self._max_deliveries, now - self._visibility_timeout),
            )
            rows = self._conn.execute(
                "SELECT id, body FROM queue_messages WHERE queue = ? AND deliveries < ? "
                "AND (claimed_at IS NULL OR claimed_at < ?) ORDER BY id LIMIT ?",
                (self.name, self._max_deliveries, now - self._visibility_timeout, limit),
            ).fetchall()
            for message_id, _ in rows:
                self._conn.execute(
                    "UPDATE queue_messages SET claimed_at = ?, deliveries = deliveries + 1 "
                    "WHERE id = ?",
                    (now, message_id),
                )
            self._conn.commit()
        return [(message_id, json.loads(body)) for message_id, body in rows]

    def ack(self, message_id: int) -> None:
        """Remove a processed message."""
        with self._lock:
            self._conn.execute("DELETE FROM queue_messages WHERE id = ?", (message_id,))
            self._conn.commit()

    def depth(self) -> int:
        """Messages waiting or in flight (excludes poison messages)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM queue_messages "
                "WHERE queue = ? AND (claimed_at IS NULL OR claimed_at >= 0)",
                (self.name,),
            ).fetchone()
        return int(row[0])

    def dead_letters(self) -> list[dict[str, Any]]:
        """Poison messages that exceeded ``max_deliveries``."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT body FROM queue_messages WHERE queue = ? AND claimed_at = -1 ORDER BY id",
                (self.name,),
            ).fetchall()
        return [json.loads(body) for (body,) in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
