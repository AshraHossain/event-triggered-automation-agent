"""Handler functions for the gitops_automation example."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from etaa.models import EventEnvelope


def start_ci(event: EventEnvelope, context: dict[str, Any]) -> dict[str, Any]:
    """Stub CI trigger — returns a fake pipeline run ID."""
    repo = event.payload.get("repository", "unknown-repo")
    branch = event.payload.get("ref", "main")
    commit = event.payload.get("head_commit", {}).get("id", "unknown")

    pipeline_id = f"pipeline-{uuid.uuid4().hex[:8]}"
    result = {
        "action": "ci_triggered",
        "pipeline_id": pipeline_id,
        "repo": repo,
        "branch": branch,
        "commit": commit,
        "triggered_at": datetime.now(UTC).isoformat(),
    }
    print(f"[CI] Pipeline {pipeline_id} triggered for {repo}@{branch} ({commit[:8]})")
    return result
