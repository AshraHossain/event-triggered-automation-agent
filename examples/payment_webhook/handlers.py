"""Handler functions for the payment_webhook example."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from etaa.models import EventEnvelope


def update_crm(event: EventEnvelope, context: dict[str, Any]) -> dict[str, Any]:
    """Stub CRM updater — records the payment in a local 'CRM' dict.

    In production this would call an actual CRM API (Salesforce, HubSpot, …).
    """
    customer_id = event.payload.get("customer_id", "unknown")
    amount = event.payload.get("amount", 0)
    currency = event.payload.get("currency", "USD")
    invoice_id = event.payload.get("invoice_id", "N/A")

    result = {
        "action": "crm_updated",
        "customer_id": customer_id,
        "amount": f"{amount} {currency}",
        "invoice_id": invoice_id,
        "updated_at": datetime.utcnow().isoformat(),
    }
    print(f"[CRM] Updated record for customer={customer_id}, amount={amount}{currency}")
    return result


def send_acknowledgment(event: EventEnvelope, context: dict[str, Any]) -> dict[str, Any]:
    """Stub: log acknowledgment dispatch."""
    return {
        "action": "ack_sent",
        "customer_email": event.payload.get("customer_email"),
        "sent_at": datetime.utcnow().isoformat(),
    }
