"""Tests for action handlers."""

from __future__ import annotations

import pytest

from etaa.actions import ActionError, AiAction, FunctionAction, HttpAction, QueueAction
from etaa.models import ActionKind, ActionSpec, EventEnvelope


@pytest.fixture
def sample_event():
    return EventEnvelope(event_id="test-1", source="demo", type="test", payload={"key": "val"})


# ─── HttpAction ───────────────────────────────────────────────────────────────


def test_http_action_fields(sample_event):
    """HttpAction has execute method and accepts ActionSpec + EventEnvelope."""
    ha = HttpAction()
    assert hasattr(ha, "execute")
    assert callable(ha.execute)


# ─── FunctionAction ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_function_action_missing_module(sample_event):
    action = ActionSpec(
        id="f1",
        type=ActionKind.FUNCTION,
        config={"module_path": "examples.nonexistent.module.func"},
    )
    fa = FunctionAction()
    with pytest.raises(ActionError, match="Cannot import module"):
        await fa.execute(action, sample_event)


# ─── QueueAction ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_queue_action_memory(sample_event):
    action = ActionSpec(
        id="q1", type=ActionKind.QUEUE, config={"queue_name": "test-q", "backend": "memory"}
    )
    qa = QueueAction()
    result = await qa.execute(action, sample_event)
    assert result.success is True
    assert qa.get_queue("test-q")


# ─── AiAction ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ai_action_stub(sample_event):
    action = ActionSpec(id="ai1", type=ActionKind.AI, config={"provider": "stub"})
    aa = AiAction()
    result = await aa.execute(action, sample_event)
    assert result.success is True
    assert result.response["provider"] == "stub"


@pytest.mark.asyncio
async def test_ai_action_unknown_provider(sample_event):
    action = ActionSpec(id="ai1", type=ActionKind.AI, config={"provider": "unknown"})
    aa = AiAction()
    with pytest.raises(ActionError, match="Unknown AI provider"):
        await aa.execute(action, sample_event)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
