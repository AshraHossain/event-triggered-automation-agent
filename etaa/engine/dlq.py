"""Dead-letter storage: write failed events, read them back, and clear them.

Two durable targets are supported — a JSONL file and a SQLite table. The
``queue`` target has no real broker behind it yet, so it falls back to a JSONL
file named after the queue: a failed event is never dropped.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from ..models import DLQConfig, DLQTargetKind
from ..observability.logger import setup_logger

log = setup_logger(__name__)

DEFAULT_DLQ_DIR = "./data/dlq"
DEFAULT_TABLE = "dlq"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _table(name: str) -> str:
    """Validate a table name; identifiers cannot be bound as SQL parameters."""
    if not _IDENTIFIER.match(name):
        raise ValueError(f"Invalid DLQ table name: {name!r}")
    return name


def _connect(db_path: str, table: str) -> sqlite3.Connection:
    name = _table(table)  # validate before opening, so a bad name leaks no connection
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {name} "
        "(id INTEGER PRIMARY KEY AUTOINCREMENT, record TEXT NOT NULL)"
    )
    return conn


def _append_file(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, default=str) + "\n")


def write(dlq: DLQConfig, record: dict[str, Any]) -> str:
    """Persist *record* to the configured target and return where it went."""
    cfg = dlq.target_config
    if dlq.target_type == DLQTargetKind.DB:
        db_path = cfg.get("db_path", f"{DEFAULT_DLQ_DIR}/dlq.db")
        table = cfg.get("table_name", DEFAULT_TABLE)
        conn = _connect(db_path, table)
        with conn:
            conn.execute(
                f"INSERT INTO {_table(table)} (record) VALUES (?)",  # nosec B608 - validated
                (json.dumps(record, default=str),),
            )
        conn.close()
        return f"{db_path}:{table}"

    if dlq.target_type == DLQTargetKind.QUEUE:
        queue_name = re.sub(r"[^A-Za-z0-9_.-]", "_", str(cfg.get("queue_name", "default")))
        path = Path(DEFAULT_DLQ_DIR) / f"{queue_name}.jsonl"
        log.warning("DLQ queue target has no broker; falling back to file %s", path)
    else:
        path = Path(cfg.get("path", f"{DEFAULT_DLQ_DIR}/default.jsonl"))
    _append_file(path, record)
    return str(path)


def load(path: str, table: str = DEFAULT_TABLE) -> list[dict[str, Any]]:
    """Return every record in a JSONL or SQLite (``.db``) dead-letter store."""
    if path.endswith(".db"):
        conn = _connect(path, table)
        rows = conn.execute(
            f"SELECT record FROM {_table(table)} ORDER BY id"  # nosec B608 - validated
        ).fetchall()
        conn.close()
        return [json.loads(row[0]) for row in rows]
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


def drop_first(path: str, count: int, table: str = DEFAULT_TABLE) -> None:
    """Remove the oldest *count* records (the ones a replay just processed).

    Records appended while the replay ran (events that failed again) are kept.
    """
    if count <= 0:
        return
    if path.endswith(".db"):
        conn = _connect(path, table)
        with conn:
            conn.execute(
                f"DELETE FROM {_table(table)} WHERE id IN "  # nosec B608 - validated
                f"(SELECT id FROM {_table(table)} ORDER BY id LIMIT ?)",
                (count,),
            )
        conn.close()
        return
    file = Path(path)
    lines = [ln for ln in file.read_text(encoding="utf-8").splitlines() if ln.strip()]
    remaining = lines[count:]
    file.write_text("".join(line + "\n" for line in remaining), encoding="utf-8")
