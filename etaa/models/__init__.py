"""Core data models for ETAA — Event-Triggered Automation Agent.

Defines the canonical schemas used throughout the system:
EventEnvelope, ActionSpec, RetryPolicy, DLQConfig, WorkflowSpec, and RunRecord.
All models use Pydantic v2 for validation and serialization.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class BackoffStrategy(StrEnum):
    """Backoff strategy for retry attempts."""

    FIXED = "fixed"
    EXPONENTIAL = "exponential"


class ActionKind(StrEnum):
    """Types of actions a workflow can execute."""

    HTTP = "http"
    QUEUE = "queue"
    FUNCTION = "function"
    AI = "ai"


class DLQTargetKind(StrEnum):
    """Target types for dead-letter queue routing."""

    QUEUE = "queue"
    FILE = "file"
    DB = "db"


class ExecutionStatus(StrEnum):
    """Status of a workflow execution run."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    DLQ = "dlq"
    SKIPPED = "skipped"  # idempotency hit — already processed


# ─── EventEnvelope ───────────────────────────────────────────────────────────


class EventEnvelope(BaseModel):
    """Canonical representation of any incoming event (webhook or queue message).

    All events flowing through ETAA are normalised to this shape so that
    workflow matching logic is source-agnostic.

    Example:
        >>> env = EventEnvelope(
        ...     event_id="evt-abc123",
        ...     source="stripe",
        ...     type="payment_succeeded",
        ...     payload={"amount": 999, "currency": "usd"},
        ... )
    """

    event_id: str = Field(..., description="Unique identifier for the event.")
    source: str = Field(..., description="Originating system, e.g. 'stripe', 'github'.")
    type: str = Field(..., description="Event type, e.g. 'payment_succeeded'.")
    payload: dict[str, Any] = Field(default_factory=dict, description="Arbitrary event payload.")
    received_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="UTC timestamp of receipt.",
    )
    metadata: dict[str, Any] = Field(default_factory=dict, description="Optional extension data.")

    def idempotency_key(self, workflow_id: str) -> str:
        """Return a deterministic key for deduplication against *workflow_id*."""
        return f"{workflow_id}:{self.event_id}"


# ─── RetryPolicy ─────────────────────────────────────────────────────────────


class RetryPolicy(BaseModel):
    """Retry behaviour configuration for an action or entire workflow.

    Example:
        >>> policy = RetryPolicy(max_attempts=3, backoff_strategy="exponential")
    """

    max_attempts: int = Field(default=3, ge=1, description="Maximum number of execution attempts.")
    backoff_strategy: BackoffStrategy = Field(
        default=BackoffStrategy.EXPONENTIAL,
        description="Backoff strategy: 'fixed' or 'exponential'.",
    )
    backoff_seconds: float = Field(
        default=1.0, gt=0, description="Base delay in seconds between retries."
    )
    backoff_cap_seconds: float = Field(
        default=60.0, gt=0, description="Upper bound on exponential backoff delay."
    )
    retryable_error_codes: list[str | int] = Field(
        default_factory=list,
        description="Error codes considered retryable. Empty means all errors are retryable.",
    )

    def compute_delay(self, attempt: int) -> float:
        """Return the delay (in seconds) before the next retry *attempt* (1-indexed)."""
        if self.backoff_strategy == BackoffStrategy.FIXED:
            return self.backoff_seconds
        # Exponential: 2^(attempt-1) * backoff_seconds, capped at backoff_cap_seconds.
        return min(self.backoff_seconds * (2 ** (attempt - 1)), self.backoff_cap_seconds)

    def should_retry(self, error_code: str | int | None, attempt: int) -> bool:
        """Decide whether another retry is warranted."""
        if attempt >= self.max_attempts:
            return False
        if not self.retryable_error_codes:
            return True
        # If no specific error code provided (generic exception), retry.
        if error_code is None:
            return True
        return str(error_code) in [str(c) for c in self.retryable_error_codes]


# ─── DLQConfig ────────────────────────────────────────────────────────────────


class DLQConfig(BaseModel):
    """Dead-letter queue configuration for a workflow.

    When an action exhausts its retry budget the event is routed here.

    Example:
        >>> dlq = DLQConfig(target_type="file", target_config={"path": "/tmp/dlq.json"})
    """

    target_type: DLQTargetKind = Field(
        default=DLQTargetKind.FILE, description="Where failed events go."
    )
    target_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Target-specific config (path, queue_name, table_name, …).",
    )
    include_payload: bool = Field(
        default=True, description="Include the original event payload in the DLQ record."
    )
    include_error: bool = Field(
        default=True, description="Include the last error details in the DLQ record."
    )


# ─── ActionSpec ───────────────────────────────────────────────────────────────


class ActionSpec(BaseModel):
    """Single step within a workflow definition.

    Example:
        >>> action = ActionSpec(id="notify_slack", type="http", config={"method": "POST"})
    """

    id: str = Field(..., description="Unique action identifier within the workflow.")
    type: ActionKind = Field(..., description="Action kind: http, queue, function, or ai.")
    config: dict[str, Any] = Field(
        default_factory=dict, description="Action-specific configuration dict."
    )
    retry_policy: RetryPolicy | None = Field(
        None, description="Per-action retry override (None → use workflow default)."
    )
    depends_on: list[str] = Field(
        default_factory=list,
        description="List of action ids that must succeed before this action runs.",
    )

    @field_validator("depends_on")
    @classmethod
    def _reject_self_dependency(cls, v: list[str], info) -> list[str]:
        """Reject an action that lists its own id as a dependency."""
        own_id = (info.data or {}).get("id")
        if own_id and own_id in v:
            raise ValueError(f"Action {own_id!r} cannot depend on itself")
        return v


# ─── WorkflowSpec ─────────────────────────────────────────────────────────────


class WorkflowTrigger(BaseModel):
    """Matching rule that determines whether a workflow fires for a given event."""

    source: str = Field(..., description="Event source to match (e.g. 'stripe').")
    type: str = Field(..., description="Event type to match (e.g. 'payment_succeeded').")
    filters: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional payload key/value filters. All keys must match.",
    )

    def matches(self, event: EventEnvelope) -> bool:
        """Return True if *event* satisfies this trigger's rules."""
        if event.source != self.source:
            return False
        if event.type != self.type:
            return False
        return all(event.payload.get(key) == expected for key, expected in self.filters.items())


class WorkflowSpec(BaseModel):
    """Declarative workflow definition loaded from YAML/JSON.

    Example YAML::

        id: payment-webhook
        name: Payment Webhook
        triggers:
          - source: stripe
            type: payment_succeeded
        actions:
          - id: send_email
            type: http
            config: {…}
    """

    id: str = Field(..., description="Unique workflow identifier.")
    name: str = Field(..., description="Human-readable name.")
    description: str = Field(default="", description="Brief description of the workflow.")
    triggers: list[WorkflowTrigger] = Field(
        default_factory=list, description="Trigger rules (OR logic)."
    )
    actions: list[ActionSpec] = Field(default_factory=list, description="Ordered list of actions.")
    retry_policy: RetryPolicy = Field(
        default_factory=RetryPolicy, description="Default retry policy."
    )
    dlq: DLQConfig = Field(
        default_factory=DLQConfig, description="Dead-letter queue configuration."
    )

    @model_validator(mode="after")
    def _validate_action_graph(self) -> WorkflowSpec:
        """Reject duplicate ids, unknown dependencies, and cycles at load time."""
        ids = [a.id for a in self.actions]
        duplicates = sorted({i for i in ids if ids.count(i) > 1})
        if duplicates:
            raise ValueError(f"duplicate action ids: {duplicates}")
        known = set(ids)
        for action in self.actions:
            unknown = sorted(set(action.depends_on) - known)
            if unknown:
                raise ValueError(f"action {action.id!r} depends on unknown actions {unknown}")
        # Kahn's algorithm: anything left over is part of a cycle.
        pending = {a.id: set(a.depends_on) for a in self.actions}
        while pending:
            ready = [i for i, deps in pending.items() if not deps]
            if not ready:
                raise ValueError(f"dependency cycle among actions {sorted(pending)}")
            for i in ready:
                del pending[i]
            for deps in pending.values():
                deps.difference_update(ready)
        return self

    def matches(self, event: EventEnvelope) -> bool:
        """Return True if *event* matches at least one trigger."""
        return any(t.matches(event) for t in self.triggers)


# ─── RunRecord ────────────────────────────────────────────────────────────────


class AttemptRecord(BaseModel):
    """Record of a single action attempt within a run."""

    action_id: str
    attempt: int
    started_at: datetime
    finished_at: datetime
    status: str  # "success" | "failed" | "resumed" (completed by an earlier attempt)
    error: str | None = None
    response: dict[str, Any] | None = None
    dlq_routed: bool = False


class RunRecord(BaseModel):
    """Persistent record of one workflow execution, used for audit & replay.

    Example:
        >>> rec = RunRecord(run_id="r1", event_id="e1", workflow_id="w1", status="success")
    """

    run_id: str = Field(..., description="Unique run identifier.")
    event_id: str = Field(..., description="The originating event ID.")
    workflow_id: str = Field(..., description="The workflow that executed.")
    status: ExecutionStatus = Field(..., description="Final execution status.")
    attempts: int = Field(default=0, description="Total number of action attempts made.")
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    metrics: dict[str, Any] = Field(default_factory=dict, description="Latency, counts, etc.")
    action_logs: list[AttemptRecord] = Field(
        default_factory=list, description="Per-attempt detail."
    )

    def finish(self, status: ExecutionStatus) -> None:
        """Mark the run finished with *status* and stamp ``finished_at``."""
        self.status = status
        self.finished_at = datetime.now(UTC)
