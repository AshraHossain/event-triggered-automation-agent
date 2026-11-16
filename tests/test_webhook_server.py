"""HTTP-layer tests for the ETAA webhook server, including signature auth."""

from __future__ import annotations

import hashlib
import hmac
import json

import pytest
from fastapi.testclient import TestClient

from etaa.models import ActionKind, ActionSpec, WorkflowSpec, WorkflowTrigger

pytestmark = pytest.mark.integration


def _queue_wf(wf_id: str, source: str, etype: str) -> WorkflowSpec:
    return WorkflowSpec(
        id=wf_id,
        name=wf_id,
        triggers=[WorkflowTrigger(source=source, type=etype)],
        actions=[
            ActionSpec(id="enqueue", type=ActionKind.QUEUE, config={"queue_name": "q"}),
        ],
    )


def _sign(secret: str, raw: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()


def test_health_reports_workflow_count(app_env):
    app, router, _ = app_env
    router.add_workflow(_queue_wf("wf", "stripe", "ping"))
    client = TestClient(app)
    resp = client.get("/api/v1/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "workflows_registered": 1}


def test_webhook_dispatches_matching_workflow(app_env):
    app, router, _ = app_env
    router.add_workflow(_queue_wf("wf", "stripe", "payment_succeeded"))
    client = TestClient(app)
    resp = client.post(
        "/api/v1/webhook/stripe",
        json={"id": "e1", "type": "payment_succeeded", "data": {"amount": 1}},
    )
    assert resp.status_code == 200
    assert resp.json()["runs"] == ["success"]


def test_malformed_json_returns_400(app_env):
    app, _, _ = app_env
    client = TestClient(app)
    resp = client.post(
        "/api/v1/webhook/stripe",
        content=b"not json",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


def test_path_source_overrides_body_source(app_env):
    """A body claiming a different source must route under the verified path source."""
    app, router, _ = app_env
    router.add_workflow(_queue_wf("wf", "stripe", "evt"))
    client = TestClient(app)
    # Body says source=github, but path says stripe → must match the stripe workflow.
    resp = client.post(
        "/api/v1/webhook/stripe",
        json={"id": "e1", "source": "github", "type": "evt"},
    )
    assert resp.json()["runs"] == ["success"]


def test_valid_signature_accepted(app_env, monkeypatch):
    monkeypatch.setenv("ETAA_WEBHOOK_SECRET_STRIPE", "shh")
    app, router, _ = app_env
    router.add_workflow(_queue_wf("wf", "stripe", "evt"))
    client = TestClient(app)
    raw = json.dumps({"id": "e1", "type": "evt"}).encode()
    resp = client.post(
        "/api/v1/webhook/stripe",
        content=raw,
        headers={"X-ETAA-Signature": _sign("shh", raw), "Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    assert resp.json()["runs"] == ["success"]


def test_invalid_signature_rejected(app_env, monkeypatch):
    monkeypatch.setenv("ETAA_WEBHOOK_SECRET_STRIPE", "shh")
    app, _, _ = app_env
    client = TestClient(app)
    resp = client.post(
        "/api/v1/webhook/stripe",
        content=b"{}",
        headers={"X-ETAA-Signature": "sha256=deadbeef", "Content-Type": "application/json"},
    )
    assert resp.status_code == 401


def test_missing_signature_fails_closed_for_other_source(app_env, monkeypatch):
    """With a secret set for one source, an unconfigured source must fail closed."""
    monkeypatch.setenv("ETAA_WEBHOOK_SECRET_STRIPE", "shh")
    app, _, _ = app_env
    client = TestClient(app)
    resp = client.post("/api/v1/webhook/github", json={"id": "e1", "type": "push"})
    assert resp.status_code == 401


def test_unsigned_source_allowlist(app_env, monkeypatch):
    monkeypatch.setenv("ETAA_WEBHOOK_SECRET_STRIPE", "shh")
    monkeypatch.setenv("ETAA_WEBHOOK_UNSIGNED_SOURCES", "github")
    app, router, _ = app_env
    router.add_workflow(_queue_wf("wf", "github", "push"))
    client = TestClient(app)
    resp = client.post("/api/v1/webhook/github", json={"id": "e1", "type": "push"})
    assert resp.status_code == 200


def test_generic_endpoint_resolves_source_from_body(app_env):
    app, router, _ = app_env
    router.add_workflow(_queue_wf("wf", "github", "push"))
    client = TestClient(app)
    resp = client.post("/api/v1/webhook", json={"id": "e1", "source": "github", "type": "push"})
    assert resp.json()["runs"] == ["success"]


def test_workflows_and_metrics_endpoints(app_env):
    app, router, _ = app_env
    router.add_workflow(_queue_wf("wf", "stripe", "evt"))
    client = TestClient(app)
    assert client.get("/api/v1/workflows").status_code == 200
    assert client.get("/api/v1/metrics").status_code == 200
    assert client.get("/api/v1/runs").status_code == 200
