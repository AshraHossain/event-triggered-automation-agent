"""Idempotency manager — prevents duplicate workflow executions for the same event."""

from __future__ import annotations

import threading
import uuid
from datetime import datetime, timedelta
from typing import Optional

from ..observability.logger import setup_logger

log = setup_logger(__name__)


class IdempotencyManager:
    """Track which (workflow_id, event_id) pairs have already been processed.

    In-memory TTL store suitable for single-process deployments.  A persistent
    backing (Redis, DB) can replace the internal dict for production use.

    Example:
        >>> mgr = IdempotencyManager(ttl_seconds=3600)
        >>> seen = mgr.check_and_set("pay-wf", "evt-abc")
        >>> assert seen is False   # first time
        >>> seen = mgr.check_and_set("pay-wf", "evt-abc")
        >>> assert seen is True    # duplicate — skip
    """

    def __init__(self, ttl_seconds: int = 3600) -> None:
        self._ttl = timedelta(seconds=ttl_seconds)
        self._seen: dict[str, datetime] = {}
        self._lock = threading.Lock()

    def check_and_set(self, workflow_id: str, event_id: str) -> bool:
        """Return True if this (workflow, event) pair has ALREADY been processed."""
        key = f"{workflow_id}:{event_id}"
        now = datetime.utcnow()
        with self._lock:
            # Prune expired entries
            expired = [k for k, ts in self._seen.items() if now - ts > self._ttl]
            for k in expired:
                del self._seen[k]

            if key in self._seen:
                return True
            self._seen[key] = now
            return False

    def clear(self) -> None:
        """Remove all recorded ids (useful for tests)."""
        with self._lock:
            self._seen.clear()

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._seen)


# Singleton helper
_default_manager: IdempotencyManager | None = None


def get_idempotency_manager(ttl_seconds: int = 3600) -> IdempotencyManager:
    global _default_manager
    if _default_manager is None:
        _default_manager = IdempotencyManager(ttl_seconds=ttl_seconds)
    return _default_manager
