"""Handler functions for the support_ticket_flow example."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from etaa.models import EventEnvelope

OWNERS = {
    "billing": "carol-billing",
    "technical": "bob-tech",
    "account": "alice-support",
}
DEFAULT_OWNER = "dave-general"


def assign_owner(event: EventEnvelope, context: dict[str, Any]) -> dict[str, Any]:
    """Route the ticket to an owner based on the classification from the AI step."""
    category = context["steps"].get("classify_ticket", {}).get("classification", "general")
    return {
        "action": "owner_assigned",
        "ticket_id": event.payload.get("ticket_id", "unknown"),
        "category": category,
        "assigned_to": OWNERS.get(category, DEFAULT_OWNER),
        "assigned_at": datetime.now(UTC).isoformat(),
    }
