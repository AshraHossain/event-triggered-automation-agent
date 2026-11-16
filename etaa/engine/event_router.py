"""Event router — matches incoming events to matching workflow specs."""

from __future__ import annotations

from ..models import EventEnvelope, WorkflowSpec
from ..observability.logger import setup_logger

log = setup_logger(__name__)


class EventRouter:
    """Route an EventEnvelope to all workflows whose triggers match.

    Matching uses OR semantics across triggers within a workflow and AND
    semantics across source, type, and optional payload filters.

    Example:
        >>> router = EventRouter()
        >>> router.add_workflow(workflow_a)
        >>> router.add_workflow(workflow_b)
        >>> matches = router.resolve(event)
        >>> for wf in matches:
        ...     print(wf.id)
    """

    def __init__(self) -> None:
        self._workflows: list[WorkflowSpec] = []

    def add_workflow(self, workflow: WorkflowSpec) -> None:
        """Register a workflow for matching."""
        self._workflows.append(workflow)
        log.info("Registered workflow: %s (%s)", workflow.id, workflow.name)

    def resolve(self, event: EventEnvelope) -> list[WorkflowSpec]:
        """Return all workflows that match *event*."""
        matched: list[WorkflowSpec] = []
        for wf in self._workflows:
            if wf.matches(event):
                matched.append(wf)
        log.info(
            "Event %s (%s:%s) matched %d workflow(s)",
            event.event_id,
            event.source,
            event.type,
            len(matched),
        )
        return matched

    def remove_workflow(self, workflow_id: str) -> bool:
        """Remove a workflow by ID. Returns True if found and removed."""
        before = len(self._workflows)
        self._workflows = [wf for wf in self._workflows if wf.id != workflow_id]
        removed = len(self._workflows) < before
        if removed:
            log.info("Removed workflow: %s", workflow_id)
        return removed

    def get_workflow(self, workflow_id: str) -> WorkflowSpec | None:
        """Return the registered workflow with *workflow_id*, if any."""
        return next((wf for wf in self._workflows if wf.id == workflow_id), None)

    @property
    def workflow_count(self) -> int:
        return len(self._workflows)

    def list_workflows(self) -> list[WorkflowSpec]:
        return list(self._workflows)
