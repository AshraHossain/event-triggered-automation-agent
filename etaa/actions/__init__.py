"""Action handlers — execute individual workflow steps.

Each action type implements a common ``execute()`` signature so the engine
can dispatch them uniformly.  Errors are wrapped in ``ActionError`` so the
engine's retry/DLQ machinery can act on them.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import httpx

from ..models import ActionSpec, EventEnvelope
from ..observability.logger import setup_logger

log = setup_logger(__name__)


@dataclass
class ActionResult:
    """Outcome of a single action execution."""

    action_id: str
    success: bool
    response: dict[str, Any] | None = None
    error: str | None = None
    elapsed_seconds: float = 0.0
    executed_at: datetime = field(default_factory=datetime.utcnow)


class ActionError(Exception):
    """Raised when an action fails. Carries optional error_code for retry logic."""

    def __init__(self, message: str, error_code: str | int | None = None, response: dict | None = None):
        super().__init__(message)
        self.error_code = error_code
        self.response = response


class BaseAction(ABC):
    """Abstract base for all action types."""

    @abstractmethod
    async def execute(self, action: ActionSpec, event: EventEnvelope) -> ActionResult:
        ...


# ─── HTTP Action ──────────────────────────────────────────────────────────────

class HttpAction(BaseAction):
    """Perform an HTTP request. Supports method, url, headers, body, timeout, expected_status_codes.

    Example config::

        {
            "method": "POST",
            "url": "https://api.example.com/notify",
            "headers": {"Authorization": "Bearer <token>"},
            "body": {"event_id": "{{event_id}}"},
            "timeout": 10,
            "expected_status_codes": [200, 201]
        }

    Template variables ``{{event_id}}``, ``{{source}}``, ``{{type}}`` are filled
    from the event envelope before the request is sent.
    """

    async def execute(self, action: ActionSpec, event: EventEnvelope) -> ActionResult:
        cfg = action.config
        method = cfg.get("method", "GET").upper()
        url = self._template(cfg.get("url", ""), event)
        headers = {k: self._template(v, event) for k, v in cfg.get("headers", {}).items()}
        body = self._template_body(cfg.get("body"), event)
        timeout = cfg.get("timeout", 30.0)
        expected = set(cfg.get("expected_status_codes", [200, 201, 204]))

        started = datetime.utcnow()
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                kwargs: dict[str, Any] = {"headers": headers}
                if method == "GET":
                    resp = await client.get(url, **kwargs)
                elif method == "POST":
                    resp = await client.post(url, json=body, **kwargs)
                elif method == "PUT":
                    resp = await client.put(url, json=body, **kwargs)
                elif method == "DELETE":
                    resp = await client.delete(url, **kwargs)
                elif method == "PATCH":
                    resp = await client.patch(url, json=body, **kwargs)
                else:
                    raise ActionError(f"Unsupported HTTP method: {method}", error_code="unsupported_method")

                if resp.status_code not in expected:
                    raise ActionError(
                        f"Unexpected status {resp.status_code}",
                        error_code=resp.status_code,
                        response={"status": resp.status_code, "body": resp.text},
                    )

                try:
                    resp_json = resp.json()
                except Exception:
                    resp_json = None

                return ActionResult(
                    action_id=action.id,
                    success=True,
                    response={"status": resp.status_code, "body": resp_json},
                    elapsed_seconds=(datetime.utcnow() - started).total_seconds(),
                )
        except httpx.TimeoutException:
            raise ActionError("Request timed out", error_code="timeout", response={"timeout": timeout})
        except httpx.ConnectError as exc:
            raise ActionError(f"Connection error: {exc}", error_code="connection_error")
        except ActionError:
            raise
        except Exception as exc:
            raise ActionError(str(exc), error_code="unknown")

    @staticmethod
    def _template(text: str, event: EventEnvelope) -> str:
        """Replace {{variable}} and {{payload.key.subkey}} placeholders with event fields."""
        if not isinstance(text, str):
            return text

        import re

        # First, handle payload field access: {{payload.customer_email}}, {{payload.head_commit.id}}
        def _resolve_payload(match):
            key_path = match.group(1)  # e.g. "customer_email" or "head_commit.id"
            parts = key_path.split(".")
            val: Any = event.payload
            for part in parts:
                if isinstance(val, dict) and part in val:
                    val = val[part]
                else:
                    return ""
            return str(val) if val is not None else ""

        result = re.sub(r"\{\{payload\.([^}]+)\}\}", _resolve_payload, text)

        # Then standard event field templates
        result = result.replace("{{event_id}}", event.event_id)
        result = result.replace("{{source}}", event.source)
        result = result.replace("{{type}}", event.type)
        return result

    @staticmethod
    def _template_body(body: Any, event: EventEnvelope) -> Any:
        if isinstance(body, dict):
            return {k: HttpAction._template(str(v), event) for k, v in body.items()}
        if isinstance(body, list):
            return [HttpAction._template_body(item, event) for item in body]
        return HttpAction._template(str(body), event) if isinstance(body, (str, int, float, bool)) else body


# ─── Queue Action ─────────────────────────────────────────────────────────────

class QueueAction(BaseAction):
    """Publish a message to a queue.

    The *config* dict must contain a ``queue_name`` key.  An optional
    ``backend`` key selects the implementation: ``"memory"`` (default for demo)
    or ``"sqs"`` / ``"rabbitmq"`` (TODO: real adapters).
    """

    def __init__(self) -> None:
        self._queues: dict[str, list[dict[str, Any]]] = {}

    async def execute(self, action: ActionSpec, event: EventEnvelope) -> ActionResult:
        cfg = action.config
        queue_name = cfg.get("queue_name", "default")
        message = self._build_message(action, event)

        backend = cfg.get("backend", "memory")
        if backend == "memory":
            self._queues.setdefault(queue_name, []).append(message)
            return ActionResult(action_id=action.id, success=True, response={"queue": queue_name, "msg_id": message.get("id")})
        elif backend in ("sqs", "rabbitmq"):
            # TODO: integrate real SQS / RabbitMQ client
            raise ActionError(f"{backend} queue backend not yet implemented", error_code="not_implemented")
        else:
            raise ActionError(f"Unknown queue backend: {backend}", error_code="invalid_config")

    def _build_message(self, action: ActionSpec, event: EventEnvelope) -> dict[str, Any]:
        return {
            "id": event.event_id,
            "source": event.source,
            "type": event.type,
            "payload": event.payload,
            "acted_by": action.id,
            "enqueued_at": datetime.utcnow().isoformat(),
        }

    def get_queue(self, name: str) -> list[dict[str, Any]]:
        return list(self._queues.get(name, []))


# ─── Function Action ──────────────────────────────────────────────────────────

class FunctionAction(BaseAction):
    """Call a local Python function by dotted module path.

    Config example::

        {
            "module_path": "examples.payment_webhook.handlers.update_crm",
            "args": ["{{payload.customer_id}}"]
        }

    The function receives ``(event: EventEnvelope, context: dict)`` as arguments.
    """

    async def execute(self, action: ActionSpec, event: EventEnvelope) -> ActionResult:
        cfg = action.config
        module_path: str = cfg.get("module_path", "")
        if not module_path:
            raise ActionError("module_path is required in config", error_code="missing_config")

        try:
            mod_name, func_name = module_path.rsplit(".", 1)
        except ValueError:
            raise ActionError(f"Invalid module_path: {module_path}", error_code="invalid_config")

        try:
            import importlib
            mod = importlib.import_module(mod_name)
        except ImportError as exc:
            raise ActionError(f"Cannot import module '{mod_name}': {exc}", error_code="import_error")

        func = getattr(mod, func_name, None)
        if func is None:
            raise ActionError(f"Function '{func_name}' not found in module '{mod_name}'", error_code="function_not_found")

        try:
            started = datetime.utcnow()
            result = func(event=event, context={"action_id": action.id, "workflow_config": cfg})
            return ActionResult(
                action_id=action.id,
                success=True,
                response={"result": result},
                elapsed_seconds=(datetime.utcnow() - started).total_seconds(),
            )
        except ActionError:
            raise
        except Exception as exc:
            raise ActionError(str(exc), error_code="function_error")


# ─── AI Action ────────────────────────────────────────────────────────────────

class AiAction(BaseAction):
    """AI action — calls an LLM endpoint.

    Config example::

        {
            "provider": "openai",
            "model": "gpt-4o-mini",
            "prompt": "Classify this ticket: {{payload.description}}",
            "max_tokens": 100,
            "temperature": 0.2
        }

    If ``OPENAI_API_KEY`` is not set in the environment, falls back to the
    deterministic stub so that demos and tests work without real API access.
    """

    PROVIDERS = ("openai", "anthropic", "stub")

    async def execute(self, action: ActionSpec, event: EventEnvelope) -> ActionResult:
        cfg = action.config
        provider = cfg.get("provider", "stub")
        if provider not in self.PROVIDERS:
            raise ActionError(f"Unknown AI provider: {provider}", error_code="invalid_config")

        started = datetime.utcnow()

        if provider == "stub":
            # Deterministic stub for demo purposes
            import hashlib
            stub_response = hashlib.sha256(f"{event.payload}".encode()).hexdigest()[:16]
            return ActionResult(
                action_id=action.id,
                success=True,
                response={"provider": "stub", "classification": stub_response},
                elapsed_seconds=0.01,
            )

        if provider == "openai":
            return await self._call_openai(cfg, event, started, action.id)

        # TODO: implement real Anthropic integration
        raise ActionError(f"Real {provider} integration not yet implemented", error_code="not_implemented")

    @staticmethod
    async def _call_openai(cfg: dict, event: EventEnvelope, started: datetime, action_id: str) -> ActionResult:
        """Call the OpenAI chat completions API.

        Requires the ``openai`` package (``pip install openai``) and an
        ``OPENAI_API_KEY`` environment variable.  If the key is missing,
        falls back to a deterministic stub.
        """
        import os
        import re

        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            # Fall back to stub so demos work without a key
            import hashlib
            stub_response = hashlib.sha256(f"{event.payload}".encode()).hexdigest()[:16]
            log.info("No OPENAI_API_KEY set; using deterministic stub response")
            return ActionResult(
                action_id=action_id,
                success=True,
                response={"provider": "openai", "model": cfg.get("model", "gpt-4o-mini"), "classification": stub_response},
                elapsed_seconds=(datetime.utcnow() - started).total_seconds(),
            )

        try:
            from openai import AsyncOpenAI
        except ImportError:
            raise ActionError(
                "openai package not installed. Run: pip install openai",
                error_code="missing_dependency",
            )

        client = AsyncOpenAI(api_key=api_key, base_url=cfg.get("base_url"))

        prompt = cfg.get("prompt", "")
        # Resolve template variables
        prompt = prompt.replace("{{event_id}}", event.event_id)
        prompt = prompt.replace("{{source}}", event.source)
        prompt = prompt.replace("{{type}}", event.type)
        # Replace {{payload.<key>}} references
        if "{{payload" in prompt:
            def _replacer(m):
                key_path = m.group(1)
                parts = key_path.split(".")[1:]  # skip "payload"
                val: Any = event.payload
                for p in parts:
                    val = val.get(p, "") if isinstance(val, dict) else ""
                return str(val)
            prompt = re.sub(r"\{\{payload\.([^}]+)\}\}", _replacer, prompt)

        messages = [{"role": "user", "content": prompt}]
        response = await client.chat.completions.create(
            model=cfg.get("model", "gpt-4o-mini"),
            messages=messages,
            max_tokens=cfg.get("max_tokens", 100),
            temperature=cfg.get("temperature", 0.7),
        )

        result_text = response.choices[0].message.content.strip() if response.choices else ""
        return ActionResult(
            action_id=action_id,
            success=True,
            response={"provider": "openai", "model": cfg.get("model"), "classification": result_text},
            elapsed_seconds=(datetime.utcnow() - started).total_seconds(),
        )