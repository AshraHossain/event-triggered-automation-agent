"""Handler functions for the support_ticket_flow example."""

from __future__ import annotations

import random
from datetime import UTC, datetime
from typing import Any

from etaa.models import EventEnvelope

OWNER_POOL = ["alice-support", "bob-tech", "carol-billing", "dave-general"]


def assign_owner(event: EventEnvelope, context: dict[str, Any]) -> dict[str, Any]:
    """Assign a random owner based on the ticket category (from AI classification)."""
    classification = context.get("classification", "general")
    # Route by category
    route = {
        "billing": "carol-billing",
        "technical": "bob-tech",
        "account": "alice-support",
    }
    owner = route.get(classification, random.choice(OWNER_POOL))

    result = {
        "action": "owner_assigned",
        "ticket_id": event.payload.get("ticket_id", "unknown"),
        "category": classification,
        "assigned_to": owner,
        "assigned_at": datetime.now(UTC).isoformat(),
    }
    print(
        f"[Support] Ticket {result['ticket_id']} categorized as "
        f"'{classification}', assigned to {owner}"
    )
    return result
