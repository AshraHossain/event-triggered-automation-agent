"""Placeholder rendering shared by all action handlers.

Supported placeholders:

- ``{{event_id}}``, ``{{source}}``, ``{{type}}`` — event envelope fields
- ``{{payload.<dotted.path>}}`` — a value from the event payload
- ``{{steps.<action_id>.<dotted.path>}}`` — output of an earlier action in the run
- ``{{env.ETAA_SECRET_<NAME>}}`` — a secret from the environment

Unknown placeholders are left untouched so typos are visible in the output.
"""

from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import quote

from .models import EventEnvelope
from .observability.logger import setup_logger

log = setup_logger(__name__)

# Only env vars with this prefix can be read from a workflow spec, so a spec
# cannot exfiltrate arbitrary process environment (API keys, signing secrets).
SECRET_ENV_PREFIX = "ETAA_SECRET_"  # nosec B105

_PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z_][\w.\-]*)\s*\}\}")
_EVENT_FIELDS = ("event_id", "source", "type")


def _dig(root: Any, dotted: str) -> Any:
    """Walk *dotted* through nested dicts; return None when any hop is missing."""
    value = root
    for part in dotted.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def render(
    text: str,
    event: EventEnvelope,
    steps: dict[str, Any] | None = None,
    *,
    url_safe: bool = False,
) -> str:
    """Substitute placeholders in *text*.

    With ``url_safe=True`` every substituted value is percent-encoded, so
    attacker-controlled payload data cannot inject path segments, query
    parameters, or a different host into an outbound URL.
    """

    def _replace(match: re.Match[str]) -> str:
        key = match.group(1)
        head, _, rest = key.partition(".")
        if key in _EVENT_FIELDS:
            value: Any = getattr(event, key)
        elif head == "payload" and rest:
            value = _dig(event.payload, rest)
        elif head == "steps" and rest:
            value = _dig(steps or {}, rest)
        elif head == "env" and rest:
            if not rest.startswith(SECRET_ENV_PREFIX):
                log.warning("Refusing to render non-secret env placeholder: %s", rest)
                return ""
            value = os.environ.get(rest)
        else:
            return match.group(0)
        rendered = "" if value is None else str(value)
        return quote(rendered, safe="") if url_safe else rendered

    return _PLACEHOLDER.sub(_replace, text)


def render_value(value: Any, event: EventEnvelope, steps: dict[str, Any] | None = None) -> Any:
    """Recursively render placeholders in strings inside dicts and lists."""
    if isinstance(value, str):
        return render(value, event, steps)
    if isinstance(value, dict):
        return {k: render_value(v, event, steps) for k, v in value.items()}
    if isinstance(value, list):
        return [render_value(v, event, steps) for v in value]
    return value
