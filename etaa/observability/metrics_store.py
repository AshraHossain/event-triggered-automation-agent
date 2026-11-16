"""In-memory + file-backed metrics store for ETAA.

Tracks per-workflow counters and latency statistics. Designed to be swapped
for a real backend (Redis, Prometheus, etc.) later.
"""

from __future__ import annotations

import sqlite3
import threading
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from ..observability.logger import setup_logger

log = setup_logger(__name__)


class MetricsStore:
    """Collects and persists execution metrics.

    The in-memory store is always updated; optionally also flushes to SQLite
    when *persist_path* is set.

    Example:
        >>> store = MetricsStore(persist_path="metrics.db")
        >>> store.increment("payment_webhook", "success_count")
        >>> store.record_latency("payment_webhook", 0.342)
        >>> store.snapshot()
    """

    def __init__(self, persist_path: str | None = None) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._latencies: dict[str, list[float]] = defaultdict(list)
        self._persist_path = persist_path
        self._conn: sqlite3.Connection | None = None
        if persist_path:
            self._connect_db(persist_path)

    def _connect_db(self, path: str) -> None:
        self._conn = sqlite3.connect(path)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                wf_id   TEXT,
                metric  TEXT,
                value   INTEGER,
                ts      TEXT
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS latencies (
                wf_id   TEXT,
                elapsed REAL,
                ts      TEXT
            )
        """)
        self._conn.commit()

    def increment(self, workflow_id: str, metric: str, value: int = 1) -> None:
        with self._lock:
            self._counters[workflow_id][metric] += value
        if self._conn:
            self._flush_counter(workflow_id, metric, value)

    def record_latency(self, workflow_id: str, elapsed_seconds: float) -> None:
        with self._lock:
            self._latencies[workflow_id].append(elapsed_seconds)
        if self._conn:
            self._flush_latency(workflow_id, elapsed_seconds)

    def _flush_counter(self, wf: str, metric: str, value: int) -> None:
        try:
            self._conn.execute(
                "INSERT INTO metrics (wf_id, metric, value, ts) VALUES (?, ?, ?, ?)",
                (wf, metric, value, datetime.now(UTC).isoformat()),
            )
            self._conn.commit()
        except Exception as exc:  # pragma: no cover — defensive
            log.warning("Failed to flush counter", exc_info=exc)

    def _flush_latency(self, wf: str, elapsed: float) -> None:
        try:
            self._conn.execute(
                "INSERT INTO latencies (wf_id, elapsed, ts) VALUES (?, ?, ?)",
                (wf, elapsed, datetime.now(UTC).isoformat()),
            )
            self._conn.commit()
        except Exception as exc:
            log.warning("Failed to flush latency", exc_info=exc)

    def snapshot(self) -> dict[str, Any]:
        """Return a full snapshot of current counters and latency stats."""
        with self._lock:
            result: dict[str, Any] = {}
            for wf_id, counters in self._counters.items():
                stats: dict[str, Any] = dict(counters)
                lats = self._latencies.get(wf_id, [])
                if lats:
                    stats["latency_min"] = min(lats)
                    stats["latency_avg"] = sum(lats) / len(lats)
                    stats["latency_max"] = max(lats)
                    stats["latency_samples"] = len(lats)
                result[wf_id] = stats
            return result

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


# Module-level singleton
_default_store: MetricsStore | None = None


def get_metrics_store(persist_path: str | None = None) -> MetricsStore:
    """Return (or create) the singleton MetricsStore."""
    global _default_store
    if _default_store is None:
        _default_store = MetricsStore(persist_path=persist_path)
    return _default_store
