"""Function action — call a local Python function by dotted path."""

from __future__ import annotations

import importlib
import os
from datetime import UTC, datetime

from ..models import ActionSpec, EventEnvelope
from .base import ActionError, ActionResult, BaseAction, Steps


class FunctionAction(BaseAction):
    """Call ``module.path.function(event=..., context=...)``.

    ``context`` carries ``action_id``, ``workflow_config`` and ``steps`` (the
    outputs of earlier actions in the run, keyed by action id).

    Security:
        ``module_path`` comes from the workflow spec, so imports are restricted
        to a prefix allowlist: ``("examples.", "etaa.")`` by default, or the
        comma-separated ``ETAA_FUNCTION_ALLOWLIST``. ``*`` allows any module
        (not recommended).
    """

    DEFAULT_ALLOWLIST = ("examples.", "etaa.")

    @classmethod
    def _allowed_prefixes(cls) -> tuple[str, ...]:
        raw = os.environ.get("ETAA_FUNCTION_ALLOWLIST")
        if raw is None:
            return cls.DEFAULT_ALLOWLIST
        return tuple(p.strip() for p in raw.split(",") if p.strip())

    @classmethod
    def _is_allowed(cls, module_path: str) -> bool:
        prefixes = cls._allowed_prefixes()
        return "*" in prefixes or any(module_path.startswith(p) for p in prefixes)

    async def execute(
        self, action: ActionSpec, event: EventEnvelope, steps: Steps | None = None
    ) -> ActionResult:
        cfg = action.config
        module_path: str = cfg.get("module_path", "")
        if not module_path or "." not in module_path:
            raise ActionError(
                f"Invalid or missing module_path: {module_path!r}",
                error_code="invalid_config",
                retryable=False,
            )
        if not self._is_allowed(module_path):
            raise ActionError(
                f"module_path '{module_path}' is not in the allowlist",
                error_code="forbidden_module",
                retryable=False,
            )

        mod_name, func_name = module_path.rsplit(".", 1)
        try:
            mod = importlib.import_module(mod_name)
        except ImportError as exc:
            raise ActionError(
                f"Cannot import module '{mod_name}': {exc}",
                error_code="import_error",
                retryable=False,
            ) from exc
        func = getattr(mod, func_name, None)
        if func is None:
            raise ActionError(
                f"Function '{func_name}' not found in module '{mod_name}'",
                error_code="function_not_found",
                retryable=False,
            )

        started = datetime.now(UTC)
        context = {"action_id": action.id, "workflow_config": cfg, "steps": steps or {}}
        try:
            result = func(event=event, context=context)
        except ActionError:
            raise
        except Exception as exc:
            raise ActionError(str(exc), error_code="function_error") from exc
        return ActionResult(
            action_id=action.id,
            success=True,
            response={"result": result},
            elapsed_seconds=(datetime.now(UTC) - started).total_seconds(),
        )
