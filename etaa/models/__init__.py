"""Core data models for ETAA — Event-Triggered Automation Agent.

Defines the canonical schemas used throughout the system:
EventEnvelope, ActionSpec, RetryPolicy, DLQConfig, WorkflowSpec, and RunRecord.
All models use Pydantic v2 for validation and serialization.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


class BackoffStrategy(str, Enum):
    """Backoff strategy for retry attempts."""
    FIXED = "fixed"
    EXPONENTIAL = "exponential"


class ActionKind(str, Enum):
    """Types of actions a workflow can execute."""
    HTTP = "http"
    QUEUE = "queue"
    FUNCTION = "function"
    AI = "ai"


class DlgTargetKind(str, Enum):
    """Target types for dead-letter queue routing."""
    QUEUE = "queue"
    FILE = "file"
    DB = "db"


class ExecutionStatus(str, Enum):
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
    received_at: datetime = Field(default_factory=datetime.utcnow, description="UTC timestamp of receipt.")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Optional extension data.")

    def idempotency_key(self, workflow_id: str) -> str:
        """Return a deterministic key for deduplication against *workflow_id*."""
        return f"{workflow_id}:{self.event_id}"


# ─── RetryPolicy ─────────────────────────────────────────────────────────────

class RetryPolicy(BaseModel):
    """Retry behaviour configuration for an action or entire workflow.

    Example:
        >>> policy = RetryPolicy(max_attempts=3, backoff_strategy="exponential", backoff_seconds=2.0)
    """

    max_attempts: int = Field(default=3, ge=1, description="Maximum number of execution attempts.")
    backoff_strategy: BackoffStrategy = Field(
        default=BackoffStrategy.EXPONENTIAL,
        description="Backoff strategy: 'fixed' or 'exponential'.",
    )
    backoff_seconds: float = Field(default=1.0, gt=0, description="Base delay in seconds between retries.")
    retryable_error_codes: list[str | int] = Field(
        default_factory=list,
        description="Error codes (string or int) considered retryable. Empty means all retries.",
    )

    def compute_delay(self, attempt: int) -> float:
        """Return the delay (in seconds) before the next retry *attempt* (1-indexed)."""
        if self.backoff_strategy == BackoffStrategy.FIXED:
            return self.backoff_seconds
        # Exponential: 2^(attempt-1) * backoff_seconds, capped at 60s
        return min(self.backoff_seconds * (2 ** (attempt - 1)), 60.0)

    def should_retry(self, error_code: str | int | None, attempt: int) -> bool:
        """Decide whether another retry is warranted."""
        if attempt >= self.max_attempts:
            return False
        if not self.retryable_error_codes:
            return True
        # If no specific error code provided (generic exception), retry
        if error_code is None:
            return True
        return str(error_code) in [str(c) for c in self.retryable_error_codes]


# ─── DLQConfig ────────────────────────────────────────────────────────────────

class DLQConfig(BaseModel):
    """Dead-letter queue configuration for a workflow.

    When an action exhausts its retry budget the event is routed here.

    Example:
        >>> dlq = DLQConfig(target_type="file", target_config={"path": "/tmp/dlq.json"}, include_payload=True)
    """

    target_type: DlgTargetKind = Field(default=DlgTargetKind.FILE, description="Where failed events go.")
    target_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Target-specific config (path, queue_name, table_name, …).",
    )
    include_payload: bool = Field(default=True, description="Include the original event payload in the DLQ record.")
    include_error: bool = Field(default=True, description="Include the last error details in the DLQ record.")


# ─── ActionSpec ───────────────────────────────────────────────────────────────

class ActionSpec(BaseModel):
    """Single step within a workflow definition.

    Example:
        >>> action = ActionSpec(
        ...     id="notify_slack",
        ...     type="http",
        ...     config={"method": "POST", "url": "https://hooks.slack.com/...", "headers": {"Content-Type": "application/json"}},
        ... )
    """

    id: str = Field(..., description="Unique action identifier within the workflow.")
    type: ActionKind = Field(..., description="Action kind: http, queue, function, or ai.")
    config: dict[str, Any] = Field(default_factory=dict, description="Action-specific configuration dict.")
    retry_policy: Optional[RetryPolicy] = Field(None, description="Per-action retry override (None → use workflow default).")
    depends_on: list[str] = Field(
        default_factory=list,
        description="List of action ids that must succeed before this action runs.",
    )

    @field_validator("depends_on")
    @classmethod
    def _reject_self_dependency(cls, v: list[str]) -> list[str]:
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
        for key, expected in self.filters.items():
            actual = event.payload.get(key)
            if actual != expected:
                return False
        return True


class WorkflowSpec(BaseModel):
    """Declarative workflow definition loaded from YAML/JSON.

    Example YAML::

        id: payment-webhook
        name: Payment Webhook
        description: Process Stripe payments
        triggers:
          - source: stripe
            type: payment_succeeded
        actions:
          - id: send_email
            type: http
            config: {…}
        retry_policy:
          max_attempts: 3
          backoff_strategy: exponential
        dlq:
          target_type: file
          target_config: {path: /tmp/dlq.json}
    """

    id: str = Field(..., description="Unique workflow identifier.")
    name: str = Field(..., description="Human-readable name.")
    description: str = Field(default="", description="Brief description of the workflow.")
    triggers: list[WorkflowTrigger] = Field(default_factory=list, description="Trigger rules (OR logic).")
    actions: list[ActionSpec] = Field(default_factory=list, description="Ordered list of actions.")
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy, description="Default retry policy.")
    dlq: DLQConfig = Field(default_factory=DLQConfig, description="Dead-letter queue configuration.")

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
    status: str  # "success" | "failed"
    error: Optional[str] = None
    response: Optional[dict[str, Any]] = None


class RunRecord(BaseModel):
    """Persistent record of one workflow execution, used for audit & replay.

    Example:
        >>> rec = RunRecord(run_id="run-001", event_id="evt-x", workflow_id="wf-pay", status="success")
    """

    run_id: str = Field(..., description="Unique run identifier.")
    event_id: str = Field(..., description="The originating event ID.")
    workflow_id: str = Field(..., description="The workflow that executed.")
    status: ExecutionStatus = Field(..., description="Final execution status.")
    attempts: int = Field(default=0, description="Total number of action attempts made.")
    started_at: datetime = Field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None
    metrics: dict[str, Any] = Field(default_factory=dict, description="Latency, counts, etc.")
    action_logs: list[AttemptRecord] = Field(default_factory=list, description="Per-attempt detail.")

    def finish(self, status: ExecutionStatus) -> None:
        self.status = status
        self.finished_at = datetime.utcnow()
