"""Shared per-turn timing metadata for ADK tool responses."""

from __future__ import annotations

import inspect
import logging
import time
from collections.abc import Callable
from contextvars import ContextVar
from functools import wraps

from .prompt_catalog import tool_description
from .runtime_contract import AgentTool
from .tool_policy import tool_enabled

logger = logging.getLogger(__name__)

turn_started_monotonic: ContextVar[float | None] = ContextVar(
    "agent_turn_started_monotonic", default=None
)
turn_deadline_monotonic: ContextVar[float | None] = ContextVar(
    "agent_turn_deadline_monotonic", default=None,
)
idle_successful_tools: ContextVar[list[dict] | None] = ContextVar(
    "idle_successful_tools", default=None
)


def _record_idle_tool(name: str, args: dict, result: object) -> None:
    records = idle_successful_tools.get()
    if records is None or name == "record_idle_activity" or not isinstance(result, dict):
        return
    failures = {"failed", "failure", "error", "rejected", "unavailable", "unknown", "omitted"}
    if result.get("ok") is False or result.get("status") in failures:
        return
    arguments = {key: value for key, value in args.items() if key != "tool_context"}
    if name == "commit_agent_turn":
        # A bundle may report completed even when individual updates failed.
        # Keep only confirmed successful items, never attribute the rest to the model.
        selected = {}
        for key, value in arguments.items():
            if key not in result:
                continue
            response = result[key]
            if isinstance(response, dict) and isinstance(response.get("results"), list):
                response = response["results"]
            if isinstance(value, list) and isinstance(response, list):
                accepted = []
                for index, item in enumerate(response):
                    if not isinstance(item, dict) or item.get("status") in failures:
                        continue
                    position = item.get("index", index)
                    if isinstance(position, int) and 0 <= position < len(value):
                        accepted.append(value[position])
                if accepted:
                    selected[key] = accepted
            elif isinstance(response, dict) and response.get("status") not in failures:
                selected[key] = value
        if not selected:
            return
        arguments = selected
    records.append({"tool": name, "args": arguments})


def _try_record_idle_tool(function: Callable, args: tuple, kwargs: dict, result: object) -> None:
    if idle_successful_tools.get() is None:
        return
    try:
        bound = inspect.signature(function).bind_partial(*args, **kwargs)
        _record_idle_tool(function.__name__, dict(bound.arguments), result)
    except Exception:  # Recording must never change a tool's outcome.
        logger.warning("event=idle_tool_record_failed tool=%s", function.__name__, exc_info=True)


def with_turn_elapsed(function: Callable):
    """Add elapsed milliseconds to dict tool results without changing schemas."""
    @wraps(function)
    async def measured(*args, **kwargs):
        fallback_start = time.monotonic()
        start = turn_started_monotonic.get()
        result = function(*args, **kwargs)
        if inspect.isawaitable(result):
            result = await result
        elapsed = int((time.monotonic() - (start or fallback_start)) * 1000)
        if isinstance(result, dict):
            return {**result, "turn_elapsed_ms": elapsed}
        return {"result": result, "turn_elapsed_ms": elapsed}

    return measured


def agent_function_tool(function: Callable, *, include_elapsed: bool = True) -> AgentTool:
    """Build an ADK tool using the centralized description catalog."""
    description = tool_description(function.__name__)
    if inspect.iscoroutinefunction(function):
        @wraps(function)
        async def described(*args, **kwargs):
            if not tool_enabled(function.__name__):
                return {"status": "unavailable", "reason": "tool_disabled_by_operator"}
            if function.__name__ == 'commit_agent_turn':
                from .tool_batch import invoke_commit
                result = await invoke_commit(function, args, kwargs)
            else:
                result = await function(*args, **kwargs)
            _try_record_idle_tool(function, args, kwargs, result)
            return result
    else:
        @wraps(function)
        def described(*args, **kwargs):
            if not tool_enabled(function.__name__):
                return {"status": "unavailable", "reason": "tool_disabled_by_operator"}
            result = function(*args, **kwargs)
            _try_record_idle_tool(function, args, kwargs, result)
            return result
    described.__doc__ = description
    wrapped = with_turn_elapsed(described) if include_elapsed else described
    return AgentTool(wrapped, function.__name__, description)


def adk_function_tool(function: Callable, *, include_elapsed: bool = True):
    """Compatibility only; production uses framework-neutral AgentTool."""
    from .frameworks.adk import to_adk
    return to_adk(agent_function_tool(function, include_elapsed=include_elapsed))
