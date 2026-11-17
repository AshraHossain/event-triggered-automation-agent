"""HTTP-layer tests for the ETAA webhook server, including signature auth."""

from __future__ import annotations

import json
import time

import pytest
from fastapi.testclient import TestClient

from etaa.ingress import sign
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


def _signed_headers(secret: str, raw: bytes, ts: str | None = None) -> dict[str, str]:
    ts = ts or str(int(time.time()))
    return {
        "X-ETAA-Signature": sign(secret, ts, raw),
        "X-ETAA-Timestamp": ts,
        "Content-Type": "application/json",
    }


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
        "/api/v1/webhook/stripe?wait=true",
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
        "/api/v1/webhook/stripe?wait=true",
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
        "/api/v1/webhook/stripe?wait=true",
        content=raw,
        headers=_signed_headers("shh", raw),
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
    assert resp.status_code == 202


def test_generic_endpoint_resolves_source_from_body(app_env):
    app, router, _ = app_env
    router.add_workflow(_queue_wf("wf", "github", "push"))
    client = TestClient(app)
    resp = client.post(
        "/api/v1/webhook?wait=true", json={"id": "e1", "source": "github", "type": "push"}
    )
    assert resp.json()["runs"] == ["success"]


def test_workflows_and_metrics_endpoints(app_env):
    app, router, _ = app_env
    router.add_workflow(_queue_wf("wf", "stripe", "evt"))
    client = TestClient(app)
    assert client.get("/api/v1/workflows").status_code == 200
    assert client.get("/api/v1/metrics").status_code == 200
    assert client.get("/api/v1/runs").status_code == 200


def test_stale_signed_request_rejected(app_env, monkeypatch):
    monkeypatch.setenv("ETAA_WEBHOOK_SECRET_STRIPE", "shh")
    app, _, _ = app_env
    raw = b'{"id": "e1", "type": "evt"}'
    stale = str(int(time.time()) - 3600)
    resp = TestClient(app).post(
        "/api/v1/webhook/stripe", content=raw, headers=_signed_headers("shh", raw, stale)
    )
    assert resp.status_code == 401


def test_oversized_body_returns_413(app_env, monkeypatch):
    monkeypatch.setenv("ETAA_MAX_BODY_BYTES", "64")
    app, _, _ = app_env
    resp = TestClient(app).post("/api/v1/webhook/stripe", json={"id": "e1", "pad": "x" * 200})
    assert resp.status_code == 413


def test_rate_limit_returns_429(app_env, monkeypatch):
    monkeypatch.setenv("ETAA_RATE_LIMIT_PER_MINUTE", "2")
    app, _, _ = app_env
    client = TestClient(app)
    codes = [client.post("/api/v1/webhook/s", json={"id": f"e{i}"}).status_code for i in range(3)]
    assert codes == [202, 202, 429]


def test_read_endpoints_require_api_key_when_configured(app_env, monkeypatch):
    monkeypatch.setenv("ETAA_API_KEY", "k3y")
    app, _, _ = app_env
    client = TestClient(app)
    assert client.get("/api/v1/runs").status_code == 401
    assert client.get("/api/v1/metrics", headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get("/api/v1/runs", headers={"X-API-Key": "k3y"}).status_code == 200
    assert (
        client.get("/api/v1/workflows", headers={"Authorization": "Bearer k3y"}).status_code == 200
    )
    assert client.get("/api/v1/health").status_code == 200  # health stays open


def test_runs_and_prometheus_reflect_this_apps_stores(app_env):
    app, router, _ = app_env
    router.add_workflow(_queue_wf("wf", "stripe", "evt"))
    client = TestClient(app)
    client.post("/api/v1/webhook/stripe?wait=true", json={"id": "e1", "type": "evt"})
    runs = client.get("/api/v1/runs").json()
    assert [r["status"] for r in runs] == ["success"]  # one row per run, not two
    text = client.get("/api/v1/metrics/prometheus").text
    assert 'etaa_success_count{workflow="wf"} 1' in text
    assert client.get("/api/v1/runs?limit=9999").status_code == 422


def test_webhook_is_acknowledged_then_processed_in_background(app_env):
    app, router, executor = app_env
    router.add_workflow(_queue_wf("wf", "stripe", "evt"))
    client = TestClient(app)
    resp = client.post("/api/v1/webhook/stripe", json={"id": "bg-1", "type": "evt"})
    assert resp.status_code == 202
    assert resp.json() == {"event_id": "bg-1", "status": "accepted", "workflows": ["wf"]}
    # The background task has run by the time the test client returns.
    assert [r.status.value for r in executor.history.list()] == ["success"]


def test_unmatched_event_is_accepted_with_no_workflows(app_env):
    app, _, _ = app_env
    resp = TestClient(app).post("/api/v1/webhook/stripe", json={"id": "x", "type": "nothing"})
    assert resp.status_code == 202
    assert resp.json()["workflows"] == []


def test_too_many_in_flight_returns_503(app_env, monkeypatch):
    monkeypatch.setenv("ETAA_MAX_IN_FLIGHT", "0")
    app, router, _ = app_env
    router.add_workflow(_queue_wf("wf", "stripe", "evt"))
    resp = TestClient(app).post("/api/v1/webhook/stripe", json={"id": "e1", "type": "evt"})
    assert resp.status_code == 503
    assert resp.headers["Retry-After"] == "5"


def test_inbox_events_are_resumed_on_startup(make_executor, tmp_path):
    """An event accepted before a crash is processed when the server starts again."""
    from etaa.engine.state_store import Inbox
    from etaa.ingress.webhook_server import create_app
    from etaa.models import EventEnvelope

    db = str(tmp_path / "state.db")
    before_crash = Inbox(db)
    before_crash.put(EventEnvelope(event_id="crashed-1", source="stripe", type="evt", payload={}))
    before_crash.close()

    executor, router, _ = make_executor()
    router.add_workflow(_queue_wf("wf", "stripe", "evt"))
    inbox = Inbox(db)
    with TestClient(create_app(executor, router, inbox=inbox)):
        pass  # startup resumes the event; shutdown waits for it
    assert [r.event_id for r in executor.history.list()] == ["crashed-1"]
    assert inbox.pending() == []
    inbox.close()
