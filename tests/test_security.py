"""Tests for ETAA security controls: function allowlist and signature helpers."""

from __future__ import annotations

import time

import pytest

from etaa.actions import ActionError, FunctionAction
from etaa.ingress import (
    resolve_source,
    sign,
    signing_enabled,
    unsigned_sources,
    verify_signature,
    webhook_secret,
)
from etaa.models import ActionKind, ActionSpec, EventEnvelope

pytestmark = pytest.mark.unit


@pytest.fixture
def event():
    return EventEnvelope(event_id="e1", source="s", type="t", payload={})


async def test_function_allowlist_blocks_unlisted_module(event, monkeypatch):
    monkeypatch.delenv("ETAA_FUNCTION_ALLOWLIST", raising=False)
    action = ActionSpec(id="f", type=ActionKind.FUNCTION, config={"module_path": "os.system"})
    with pytest.raises(ActionError, match="not in the allowlist"):
        await FunctionAction().execute(action, event)


async def test_function_allowlist_wildcard_allows_any(event, monkeypatch):
    monkeypatch.setenv("ETAA_FUNCTION_ALLOWLIST", "*")
    action = ActionSpec(id="f", type=ActionKind.FUNCTION, config={"module_path": "nope.nope.fn"})
    # Wildcard allows the attempt; it then fails at import, not at the gate.
    with pytest.raises(ActionError, match="Cannot import module"):
        await FunctionAction().execute(action, event)


async def test_function_custom_allowlist_prefix(event, monkeypatch):
    monkeypatch.setenv("ETAA_FUNCTION_ALLOWLIST", "mypkg.")
    assert FunctionAction._is_allowed("mypkg.handlers.fn") is True
    assert FunctionAction._is_allowed("examples.handlers.fn") is False


def test_verify_signature_roundtrip():
    secret, body, ts = "shh", b'{"a":1}', str(int(time.time()))
    sig = sign(secret, ts, body)
    assert verify_signature(secret, body, sig, ts) is True
    assert verify_signature(secret, body, "sha256=bad", ts) is False
    assert verify_signature(secret, body, None, ts) is False
    assert verify_signature(secret, body, sig, None) is False
    assert verify_signature(secret, body, sig, "not-a-number") is False


def test_verify_signature_rejects_replay():
    """A correctly signed request outside the tolerance window is rejected."""
    secret, body = "shh", b"{}"
    stale = str(int(time.time()) - 3600)
    assert verify_signature(secret, body, sign(secret, stale, body), stale) is False


def test_signature_binds_timestamp():
    """Re-stamping a captured request with a fresh timestamp must fail."""
    secret, body = "shh", b"{}"
    old, fresh = str(int(time.time()) - 60), str(int(time.time()))
    assert verify_signature(secret, body, sign(secret, old, body), fresh) is False


def test_webhook_secret_precedence(monkeypatch):
    monkeypatch.setenv("ETAA_WEBHOOK_SECRET", "global")
    monkeypatch.setenv("ETAA_WEBHOOK_SECRET_STRIPE", "stripe-secret")
    assert webhook_secret("stripe") == "stripe-secret"
    assert webhook_secret("other") == "global"
    assert signing_enabled() is True


def test_signing_disabled_by_default(monkeypatch):
    for k in list(__import__("os").environ):
        if k.startswith("ETAA_WEBHOOK_SECRET"):
            monkeypatch.delenv(k, raising=False)
    assert signing_enabled() is False
    assert webhook_secret("x") is None


def test_unsigned_sources_parsing(monkeypatch):
    monkeypatch.setenv("ETAA_WEBHOOK_UNSIGNED_SOURCES", "github, internal ,")
    assert unsigned_sources() == {"github", "internal"}


def test_resolve_source_prefers_body_then_header_then_default():
    assert resolve_source({"source": "a", "x-event-source": "b"}) == "a"
    assert resolve_source({"x-event-source": "b"}) == "b"
    assert resolve_source({}) == "webhook"
