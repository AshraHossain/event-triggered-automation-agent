"""Shared types for action handlers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ..models import ActionSpec, EventEnvelope

# Outputs of earlier actions in the same run, keyed by action id.
Steps = dict[str, Any]


@dataclass(frozen=True)
class ActionResult:
    """Outcome of a single action execution."""

    action_id: str
    success: bool
    response: dict[str, Any] | None = None
    error: str | None = None
    elapsed_seconds: float = 0.0
    executed_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class ActionError(Exception):
    """Raised when an action fails.

    ``error_code`` feeds the retry policy. ``retryable=False`` marks errors that
    can never succeed on retry (bad config, forbidden target), so the engine
    routes them straight to the DLQ instead of burning the retry budget.
    """

    def __init__(
        self,
        message: str,
        error_code: str | int | None = None,
        response: dict | None = None,
        retryable: bool = True,
    ):
        super().__init__(message)
        self.error_code = error_code
        self.response = response
        self.retryable = retryable


class BaseAction(ABC):
    """Abstract base for all action types."""

    @abstractmethod
    async def execute(
        self, action: ActionSpec, event: EventEnvelope, steps: Steps | None = None
    ) -> ActionResult: ...
