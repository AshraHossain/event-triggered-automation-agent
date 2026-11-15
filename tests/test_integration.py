"""Integration tests for the full ETAA pipeline.

These tests exercise the end-to-end flow: WebhookIngress → EventRouter →
WorkflowExecutor → actions → observability stores.  HTTP actions are mocked
with respx so no real network calls are made.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest
import respx
import httpx

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etaa.actions import HttpAction, QueueAction, FunctionAction, AiAction, ActionError
from etaa.engine.event_router import EventRouter
from etaa.engine.idempotency_manager import IdempotencyManager
from etaa.engine.workflow_executor import WorkflowExecutor
from etaa.ingress import WebhookIngress
from etaa.models import (
    ActionKind,
    ActionSpec,
    DLQConfig,
    DlgTargetKind,
    EventEnvelope,
    ExecutionStatus,
    RetryPolicy,
    WorkflowSpec,
    WorkflowTrigger,
)
from etaa.observability.metrics_store import MetricsStore
from etaa.observability.run_history_store import RunHistoryStore


@pytest.fixture
def sample_event():
    return EventEnvelope(
        event_id="evt-int-test-001",
        source="stripe",
        type="payment_succeeded",
        payload={"customer_id": "cus_1", "amount": 5000, "currency": "usd", "success": True},
    )


@pytest.fixture
def full_pipeline(tmp_path):
    """Build a complete pipeline with fresh stores for each test."""
    metrics = MetricsStore()
    runs_dir = tmp_path / "runs"
    runs_dir.mkdir()
    history = RunHistoryStore(path=str(runs_dir))
    idempotency = IdempotencyManager(ttl_seconds=60)
    router = EventRouter()

    action_registry: dict[str, Any] = {
        "http": HttpAction(),
        "queue": QueueAction(),
        "function": FunctionAction(),
        "ai": AiAction(),
    }

    def action_factory(kind: str):
        return action_registry.get(kind)

    executor = WorkflowExecutor(router, action_factory, metrics, history, idempotency)

    return {
        "executor": executor,
        "router": router,
        "metrics": metrics,
        "history": history,
        "idempotency": idempotency,
        "action_registry": action_registry,
        "runs_dir": str(runs_dir),
    }


# ─── End-to-end: WebhookIngress → EventRouter → WorkflowExecutor ──────────────

@pytest.mark.asyncio
async def test_full_pipeline_http_success(full_pipeline):
    """An HTTP action with a matching 200 response should mark the run as success."""
    router = full_pipeline["router"]
    executor = full_pipeline["executor"]

    wf = WorkflowSpec(
        id="test-http-wf",
        name="HTTP Test",
        triggers=[WorkflowTrigger(source="stripe", type="payment_succeeded")],
        actions=[
            ActionSpec(
                id="call_api",
                type=ActionKind.HTTP,
                config={
                    "method": "POST",
                    "url": "https://api.example.com/notify",
                    "body": {"event_id": "{{event_id}}"},
                    "expected_status_codes": [200],
                },
            ),
        ],
    )
    router.add_workflow(wf)

    event = EventEnvelope(event_id="evt-1", source="stripe", type="payment_succeeded", payload={})

    with respx.mock:
        respx.post("https://api.example.com/notify").respond(200, json={"ok": True})
        runs = await executor.execute(event)

    assert len(runs) == 1
    run = runs[0]
    assert run.status == ExecutionStatus.SUCCESS
    assert run.attempts == 1
    assert run.action_logs[0].status == "success"
    assert full_pipeline["metrics"].snapshot()["test-http-wf"]["success_count"] == 1


@pytest.mark.asyncio
async def test_full_pipeline_idempotency(full_pipeline):
    """Sending the same event twice should produce a SKIPPED run the second time."""
    router = full_pipeline["router"]
    executor = full_pipeline["executor"]

    wf = WorkflowSpec(
        id="test-idem-wf",
        name="Idempotency Test",
        triggers=[WorkflowTrigger(source="stripe", type="test_event")],
        actions=[
            ActionSpec(id="send_msg", type=ActionKind.QUEUE, config={"queue_name": "idem-test"}),
        ],
    )
    router.add_workflow(wf)

    event = EventEnvelope(event_id="evt-dup-1", source="stripe", type="test_event", payload={})

    # First run
    runs1 = await executor.execute(event)
    # Second run (same event_id + workflow_id)
    runs2 = await executor.execute(event)

    assert runs1[0].status == ExecutionStatus.SUCCESS
    assert runs2[0].status == ExecutionStatus.SKIPPED
    assert full_pipeline["metrics"].snapshot()["test-idem-wf"]["skipped_count"] == 1


@pytest.mark.asyncio
async def test_full_pipeline_retry_then_success(full_pipeline):
    """Action fails once (500), then succeeds on retry → overall status SUCCESS."""
    router = full_pipeline["router"]
    executor = full_pipeline["executor"]

    wf = WorkflowSpec(
        id="test-retry-wf",
        name="Retry Test",
        triggers=[WorkflowTrigger(source="demo", type="retry_test")],
        actions=[
            ActionSpec(
                id="flaky_http",
                type=ActionKind.HTTP,
                config={
                    "method": "GET",
                    "url": "https://flaky.example.com/api",
                    "expected_status_codes": [200],
                },
                retry_policy=RetryPolicy(
                    max_attempts=3,
                    backoff_strategy="fixed",
                    backoff_seconds=0.01,
                    retryable_error_codes=[500, "timeout"],
                ),
            ),
        ],
    )
    router.add_workflow(wf)

    event = EventEnvelope(event_id="evt-retry-1", source="demo", type="retry_test", payload={})

    with respx.mock:
        route = respx.get("https://flaky.example.com/api")
        route.side_effect = [
            httpx.Response(500, text="Internal Error"),
            httpx.Response(200, json={"status": "ok"}),
        ]
        runs = await executor.execute(event)

    assert len(runs) == 1
    run = runs[0]
    assert run.status == ExecutionStatus.SUCCESS
    assert run.attempts == 2
    assert route.call_count == 2
    assert full_pipeline["metrics"].snapshot()["test-retry-wf"]["success_count"] == 1


@pytest.mark.asyncio
async def test_full_pipeline_dlq_after_exhaustion(full_pipeline, tmp_path):
    """When all retries fail, the event should be written to the DLQ file."""
    router = full_pipeline["router"]
    executor = full_pipeline["executor"]
    dlq_path = str(tmp_path / "dlq_test.jsonl")

    wf = WorkflowSpec(
        id="test-dlq-wf",
        name="DLQ Test",
        triggers=[WorkflowTrigger(source="demo", type="dlq_test")],
        actions=[
            ActionSpec(
                id="always_fail",
                type=ActionKind.HTTP,
                config={
                    "method": "GET",
                    "url": "https://broken.example.com/api",
                    "expected_status_codes": [200],
                },
                retry_policy=RetryPolicy(
                    max_attempts=2,
                    backoff_strategy="fixed",
                    backoff_seconds=0.01,
                    retryable_error_codes=[429, 500, "connection_error"],
                ),
            ),
        ],
        dlq=DLQConfig(
            target_type=DlgTargetKind.FILE,
            target_config={"path": dlq_path},
            include_payload=True,
            include_error=True,
        ),
    )
    router.add_workflow(wf)

    event = EventEnvelope(event_id="evt-dlq-1", source="demo", type="dlq_test", payload={"important": "data"})

    with respx.mock:
        respx.get("https://broken.example.com/api").respond(500)
        runs = await executor.execute(event)

    assert len(runs) == 1
    run = runs[0]
    assert run.status == ExecutionStatus.FAILED
    assert run.attempts == 2

    dlq_file = Path(dlq_path)
    assert dlq_file.exists()
    dlq_lines = dlq_file.read_text().strip().splitlines()
    assert len(dlq_lines) >= 1
    dlq_record = json.loads(dlq_lines[0])
    assert dlq_record["action_id"] == "always_fail"
    assert dlq_record["event"]["event_id"] == "evt-dlq-1"
    assert dlq_record["event"]["payload"]["important"] == "data"
    assert "error" in dlq_record

    assert full_pipeline["metrics"].snapshot()["test-dlq-wf"]["failure_count"] == 1


@pytest.mark.asyncio
async def test_full_pipeline_dag_ordering(full_pipeline):
    """Actions with depends_on should execute in topological order."""
    router = full_pipeline["router"]
    executor = full_pipeline["executor"]
    queue_action = full_pipeline["action_registry"]["queue"]

    # Create a synchronous function handler
    def sync_fn(event: EventEnvelope, context: dict) -> dict:
        return {"called": True, "action_id": context["action_id"]}

    import types
    mod = types.ModuleType("test_temp_handler")
    mod.test_fn = sync_fn
    sys.modules["test_temp_handler"] = mod

    wf = WorkflowSpec(
        id="test-dag-wf",
        name="DAG Test",
        triggers=[WorkflowTrigger(source="test", type="dag_test")],
        actions=[
            ActionSpec(
                id="second",
                type=ActionKind.FUNCTION,
                config={"module_path": "test_temp_handler.test_fn"},
                depends_on=["first"],
            ),
            ActionSpec(
                id="first",
                type=ActionKind.QUEUE,
                config={"queue_name": "dag-order-test"},
            ),
        ],
    )
    router.add_workflow(wf)

    event = EventEnvelope(event_id="evt-dag-1", source="test", type="dag_test", payload={})

    runs = await executor.execute(event)

    assert runs[0].status == ExecutionStatus.SUCCESS
    # The queue action ("first") should have been processed before the function ("second")
    assert len(queue_action.get_queue("dag-order-test")) > 0
    assert runs[0].action_logs[0].action_id == "first"
    assert runs[0].action_logs[1].action_id == "second"

    # Clean up
    sys.modules.pop("test_temp_handler", None)


@pytest.mark.asyncio
async def test_full_pipeline_run_history_persisted(full_pipeline):
    """Run records should be persisted to the run history store."""
    router = full_pipeline["router"]
    executor = full_pipeline["executor"]
    history = full_pipeline["history"]

    wf = WorkflowSpec(
        id="test-history-wf",
        name="History Test",
        triggers=[WorkflowTrigger(source="test", type="history_test")],
        actions=[
            ActionSpec(id="noop", type=ActionKind.QUEUE, config={"queue_name": "history-test"}),
        ],
    )
    router.add_workflow(wf)

    event = EventEnvelope(event_id="evt-history-1", source="test", type="history_test", payload={})

    runs = await executor.execute(event)

    assert len(runs) == 1
    run_id = runs[0].run_id

    retrieved = history.get(run_id)
    assert retrieved is not None
    assert retrieved.status == ExecutionStatus.SUCCESS
    assert retrieved.workflow_id == "test-history-wf"

    all_runs = history.list(workflow_id="test-history-wf", limit=10)
    assert any(r.run_id == run_id for r in all_runs)


@pytest.mark.asyncio
async def test_full_pipeline_no_match(full_pipeline):
    """An event that matches no workflow should produce 0 runs."""
    router = full_pipeline["router"]
    executor = full_pipeline["executor"]

    router.add_workflow(
        WorkflowSpec(
            id="test-match-wf",
            name="Match Test",
            triggers=[WorkflowTrigger(source="stripe", type="payment_succeeded")],
            actions=[],
        )
    )

    event = EventEnvelope(event_id="evt-nomatch-1", source="github", type="push", payload={})

    runs = await executor.execute(event)

    assert runs == []


@pytest.mark.asyncio
async def test_http_action_template_substitution(full_pipeline):
    """Template variables {{event_id}}, {{source}}, {{type}} should be substituted in URLs and body."""
    router = full_pipeline["router"]
    executor = full_pipeline["executor"]

    captured_request = {}

    wf = WorkflowSpec(
        id="test-template-wf",
        name="Template Test",
        triggers=[WorkflowTrigger(source="testsrc", type="tmpl")],  # match event source
        actions=[
            ActionSpec(
                id="templated",
                type=ActionKind.HTTP,
                config={
                    "method": "POST",
                    "url": "https://api.example.com/webhook/{{source}}",
                    "headers": {"X-Event-ID": "{{event_id}}"},
                    "body": {"event_id": "{{event_id}}", "source": "{{source}}", "type": "{{type}}"},
                    "expected_status_codes": [200],
                },
            ),
        ],
    )
    router.add_workflow(wf)

    event = EventEnvelope(event_id="tmpl-evt-1", source="testsrc", type="tmpl", payload={"data": "hello"})

    with respx.mock:
        def capture(request):
            captured_request["url"] = str(request.url)
            captured_request["headers"] = dict(request.headers)
            captured_request["body"] = json.loads(request.content)
            return httpx.Response(200, json={"ok": True})

        respx.post("https://api.example.com/webhook/testsrc").mock(side_effect=capture)
        runs = await executor.execute(event)

    assert len(runs) == 1
    assert runs[0].status == ExecutionStatus.SUCCESS
    assert captured_request["url"].endswith("/webhook/testsrc")
    assert captured_request["headers"].get("x-event-id") == "tmpl-evt-1"
    assert captured_request["body"]["event_id"] == "tmpl-evt-1"
    assert captured_request["body"]["source"] == "testsrc"
    assert captured_request["body"]["type"] == "tmpl"


@pytest.mark.asyncio
async def test_http_action_unexpected_status_code(full_pipeline):
    """A 404 response should fail the action (non-retryable by default)."""
    router = full_pipeline["router"]
    executor = full_pipeline["executor"]

    wf = WorkflowSpec(
        id="test-404-wf",
        name="404 Test",
        triggers=[WorkflowTrigger(source="test", type="not_found")],
        actions=[
            ActionSpec(
                id="failing_http",
                type=ActionKind.HTTP,
                config={
                    "method": "GET",
                    "url": "https://api.example.com/missing",
                    "expected_status_codes": [200],
                },
            ),
        ],
    )
    router.add_workflow(wf)

    event = EventEnvelope(event_id="evt-404", source="test", type="not_found", payload={})

    with respx.mock:
        respx.get("https://api.example.com/missing").respond(404)
        runs = await executor.execute(event)

    assert len(runs) == 1
    assert runs[0].status == ExecutionStatus.FAILED
    assert full_pipeline["metrics"].snapshot()["test-404-wf"]["failure_count"] == 1


@pytest.mark.asyncio
async def test_http_action_payload_template_substitution(full_pipeline):
    """{{payload.key}} should be resolved from the event payload, including nested keys."""
    router = full_pipeline["router"]
    executor = full_pipeline["executor"]

    captured_request = {}

    wf = WorkflowSpec(
        id="test-payload-template-wf",
        name="Payload Template Test",
        triggers=[WorkflowTrigger(source="stripe", type="payment_succeeded")],
        actions=[
            ActionSpec(
                id="templated",
                type=ActionKind.HTTP,
                config={
                    "method": "POST",
                    "url": "https://api.example.com/notify",
                    "body": {
                        "customer_email": "{{payload.customer_email}}",
                        "amount": "{{payload.amount}}",
                        "nested": "{{payload.head_commit.id}}",
                    },
                    "expected_status_codes": [200],
                },
            ),
        ],
    )
    router.add_workflow(wf)

    event = EventEnvelope(
        event_id="evt-tmpl-payload",
        source="stripe",
        type="payment_succeeded",
        payload={
            "customer_email": "alice@example.com",
            "amount": 9999,
            "head_commit": {"id": "abc123"},
        },
    )

    with respx.mock:
        def capture(request):
            captured_request["body"] = json.loads(request.content)
            return httpx.Response(200, json={"ok": True})

        respx.post("https://api.example.com/notify").mock(side_effect=capture)
        runs = await executor.execute(event)

    assert runs[0].status == ExecutionStatus.SUCCESS
    assert captured_request["body"]["customer_email"] == "alice@example.com"
    assert captured_request["body"]["amount"] == "9999"
    assert captured_request["body"]["nested"] == "abc123"


@pytest.mark.asyncio
async def test_ai_action_stub_classification(full_pipeline):
    """AI action with stub provider should return a deterministic classification hash."""
    router = full_pipeline["router"]
    executor = full_pipeline["executor"]

    wf = WorkflowSpec(
        id="test-ai-wf",
        name="AI Test",
        triggers=[WorkflowTrigger(source="test", type="ai_test")],
        actions=[
            ActionSpec(
                id="classify",
                type=ActionKind.AI,
                config={"provider": "stub", "model": "gpt-4o-mini"},
            ),
        ],
    )
    router.add_workflow(wf)

    event = EventEnvelope(
        event_id="evt-ai-1",
        source="test",
        type="ai_test",
        payload={"description": "billing issue"},
    )

    runs = await executor.execute(event)

    assert len(runs) == 1
    assert runs[0].status == ExecutionStatus.SUCCESS
    assert len(runs[0].action_logs) == 1
    log = runs[0].action_logs[0]
    assert log.status == "success"
    assert log.response["provider"] == "stub"
    assert "classification" in log.response
    assert len(log.response["classification"]) == 16  # sha256 hex[:16]
