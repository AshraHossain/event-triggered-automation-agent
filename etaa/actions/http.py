"""HTTP action — outbound requests with templating and an SSRF guard."""

from __future__ import annotations

import ipaddress
import os
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

import httpx

from ..models import ActionSpec, EventEnvelope
from ..templating import render, render_value
from .base import ActionError, ActionResult, BaseAction, Steps

ALLOWED_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE")
BODY_METHODS = ("POST", "PUT", "PATCH")
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_EXPECTED_STATUS = (200, 201, 204)


def _is_private_host(host: str) -> bool:
    """Return True for loopback, private, link-local, and reserved targets."""
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        # ponytail: hostnames are not resolved, so a DNS name pointing at a
        # private IP passes. Resolve-and-pin here if specs become untrusted.
        return False
    return ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved


def validate_url(url: str) -> None:
    """Reject URLs that are malformed or point somewhere outbound calls must not go.

    - Only ``http`` / ``https``.
    - ``ETAA_HTTP_ALLOWED_HOSTS`` (comma-separated), when set, is an exact-match
      host allowlist.
    - Private / loopback / link-local addresses are blocked unless
      ``ETAA_HTTP_ALLOW_PRIVATE=1`` (cloud metadata endpoints live there).
    """
    parts = urlsplit(url)
    host = parts.hostname
    if parts.scheme not in ("http", "https") or not host:
        raise ActionError(f"Invalid URL: {url!r}", error_code="invalid_url", retryable=False)

    allowed = {h.strip() for h in os.environ.get("ETAA_HTTP_ALLOWED_HOSTS", "").split(",")}
    allowed.discard("")
    if allowed and host not in allowed:
        raise ActionError(
            f"Host {host!r} is not in ETAA_HTTP_ALLOWED_HOSTS",
            error_code="forbidden_host",
            retryable=False,
        )
    if os.environ.get("ETAA_HTTP_ALLOW_PRIVATE") != "1" and _is_private_host(host):
        raise ActionError(
            f"Host {host!r} is a private or loopback address",
            error_code="forbidden_host",
            retryable=False,
        )


class HttpAction(BaseAction):
    """Perform an HTTP request.

    Example config::

        {
            "method": "POST",
            "url": "https://api.example.com/notify/{{payload.customer_id}}",
            "headers": {"Authorization": "Bearer {{env.ETAA_SECRET_API_TOKEN}}"},
            "body": {"event_id": "{{event_id}}"},
            "timeout": 10,
            "expected_status_codes": [200, 201]
        }

    Values substituted into the URL are percent-encoded. Pass ``transport`` to
    swap the network layer (demos, dry runs, tests).
    """

    def __init__(self, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._transport = transport

    async def execute(
        self, action: ActionSpec, event: EventEnvelope, steps: Steps | None = None
    ) -> ActionResult:
        cfg = action.config
        method = str(cfg.get("method", "GET")).upper()
        if method not in ALLOWED_METHODS:
            raise ActionError(
                f"Unsupported HTTP method: {method}",
                error_code="unsupported_method",
                retryable=False,
            )
        url = render(str(cfg.get("url", "")), event, steps, url_safe=True)
        validate_url(url)
        headers = {k: render(str(v), event, steps) for k, v in cfg.get("headers", {}).items()}
        body = render_value(cfg.get("body"), event, steps) if method in BODY_METHODS else None
        timeout = cfg.get("timeout", DEFAULT_TIMEOUT_SECONDS)
        expected = set(cfg.get("expected_status_codes", DEFAULT_EXPECTED_STATUS))

        started = datetime.now(UTC)
        try:
            async with httpx.AsyncClient(timeout=timeout, transport=self._transport) as client:
                resp = await client.request(method, url, headers=headers, json=body)
        except httpx.TimeoutException as exc:
            raise ActionError(
                "Request timed out", error_code="timeout", response={"timeout": timeout}
            ) from exc
        except httpx.ConnectError as exc:
            raise ActionError(f"Connection error: {exc}", error_code="connection_error") from exc
        except httpx.HTTPError as exc:
            raise ActionError(str(exc), error_code="http_error") from exc

        if resp.status_code not in expected:
            raise ActionError(
                f"Unexpected status {resp.status_code}",
                error_code=resp.status_code,
                response={"status": resp.status_code, "body": resp.text},
            )
        return ActionResult(
            action_id=action.id,
            success=True,
            response={"status": resp.status_code, "body": _json_or_none(resp)},
            elapsed_seconds=(datetime.now(UTC) - started).total_seconds(),
        )


def _json_or_none(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        return None
