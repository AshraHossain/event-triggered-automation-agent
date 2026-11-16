"""Structured JSON logging for ETAA.

Provides a logger that emits one-JSON-line records suitable for log aggregation
systems (Datadog, CloudWatch, ELK, …).
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime


class JsonFormatter(logging.Formatter):
    """Format every log record as a single JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info and record.exc_info[0] is not None:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


def setup_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Create and configure a JSON logger with the given *name* and *level*.

    Example:
        >>> log = setup_logger("etaa.engine")
        >>> log.info("workflow started", extra={"workflow_id": "pay-01"})
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    return logger
