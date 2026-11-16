"""Demo: new support ticket → classify → assign owner → acknowledgement email.

The classification from the first action is passed to the next two. The
classifier is the offline stub (keyword rules), so no API key is needed.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from examples.demo_support import run_demo  # noqa: E402


def _ticket(event_id: str, ticket_id: str, email: str, description: str) -> dict:
    return {
        "id": event_id,
        "source": "support_system",
        "type": "ticket_created",
        "data": {"ticket_id": ticket_id, "requester_email": email, "description": description},
    }


SAMPLE_EVENTS = [
    _ticket(
        "evt_ticket_001",
        "TKT-1001",
        "customer1@example.com",
        "I can't reset my password. The link doesn't work.",
    ),
    _ticket(
        "evt_ticket_002",
        "TKT-1002",
        "customer2@example.com",
        "I was charged twice for my subscription.",
    ),
]

if __name__ == "__main__":
    asyncio.run(run_demo(Path(__file__).parent, SAMPLE_EVENTS))
