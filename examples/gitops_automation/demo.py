"""Demo: GitHub push to main → start CI → notify Slack.

The second event is a push to a feature branch and matches no workflow.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from examples.demo_support import run_demo  # noqa: E402


def _push(event_id: str, ref: str, commit: str, author: str) -> dict:
    return {
        "id": event_id,
        "source": "github",
        "type": "push",
        "data": {
            "repository": "myorg/webapp",
            "ref": ref,
            "head_commit": {"id": commit, "message": "update", "author": {"name": author}},
        },
    }


SAMPLE_EVENTS = [
    _push("evt_git_001", "refs/heads/main", "abc123def456", "Jane Dev"),
    _push("evt_git_002", "refs/heads/feature/auth", "xyz789", "John Doe"),  # not main
]

if __name__ == "__main__":
    asyncio.run(run_demo(Path(__file__).parent, SAMPLE_EVENTS))
