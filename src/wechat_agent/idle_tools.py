"""Context-bound completion tool for durable idle-event summaries."""

from __future__ import annotations

import logging
from contextvars import ContextVar
from dataclasses import replace
from datetime import UTC, datetime

from .context import ContextTurn
from .prompt_catalog import load_prompt
from .runtime_contract import AgentTool as ToolUnion
from .runtime_contract import ModelCallsLimitExceeded, ToolContext
from .time_scheduler import TimeScheduler
from .tool_timing import agent_function_tool, idle_successful_tools

_idle_scope: ContextVar[tuple[str, str, int, str, datetime] | None] = ContextVar(
    "idle_event_scope",
    default=None,
)
logger = logging.getLogger(__name__)
IDLE_WORK_MAX_MODEL_CALLS = 24
IDLE_SUMMARY_MAX_MODEL_CALLS = 2


class IdleActivityToolset:
    def __init__(self, scheduler: TimeScheduler) -> None:
        self.scheduler = scheduler

    def record_idle_activity(
        self, summary: str, tool_context: ToolContext | None = None,
    ) -> dict[str, object]:
        scope = _idle_scope.get()
        if scope is None:
            return {"status": "unavailable", "reason": "not_an_idle_event"}
        account_id, event_id, event_number, model_id, started_at = scope
        try:
            activity_id = self.scheduler.record_idle_activity(
                account_id=account_id,
                event_id=event_id,
                event_number=event_number,
                model_id=model_id,
                summary=summary,
                started_at=started_at,
                finished_at=datetime.now(UTC),
                tool_calls=list(idle_successful_tools.get() or []),
            )
        except ValueError as exc:
            return {"status": "rejected", "reason": str(exc)}
        if tool_context is not None:
            tool_context.end_turn()
        return {"status": "recorded", "activity_id": activity_id}

    async def run_with_summary(self, runner, window) -> None:
        """Keep completed writes and give a capped run one summary-only continuation."""
        scope = _idle_scope.get()
        if scope is None:
            raise RuntimeError("idle activity scope is not bound")
        account_id, event_id, number, _model_id, started_at = scope
        failure = None
        try:
            await runner.reply(window, max_llm_calls=IDLE_WORK_MAX_MODEL_CALLS)
        except ModelCallsLimitExceeded:
            failure = "ModelCallsLimitExceeded"
            logger.warning("event=idle_work_call_limit event_id=%s", event_id)
        # Writes are committed by each tool. Never replay work or overwrite a
        # summary merely because a later model step hit its call limit.
        if self.scheduler.has_idle_activity(account_id, event_id):
            return
        prompt = load_prompt("idle_finalize")
        if failure:
            prompt += "\n本轮已达到模型调用次数上限。只总结已有工具回执，明确未完成事项，不再开展任务。"
        finalize = ContextTurn(
            sender_id="系统空闲调度器", sender_origin="idle_finalize", text=prompt,
            message_id=f"idle-finalize:{event_id}",
            source_conversation_id=window.conversation_id, source_is_group=False,
        )
        try:
            await runner.reply(
                replace(window, turns=(finalize,), global_timeline_turns=()),
                max_llm_calls=IDLE_SUMMARY_MAX_MODEL_CALLS,
                allowed_tool_names=frozenset({"record_idle_activity"}),
            )
        except Exception as exc:  # noqa: BLE001 - persist fallback without replaying work
            # Cancellation deliberately propagates; a new inbound message must
            # still interrupt idle work. Other failures retain the real history.
            failure = type(exc).__name__
            logger.warning("event=idle_summary_failed event_id=%s error_type=%s",
                           event_id, failure)
        if not self.scheduler.has_idle_activity(account_id, event_id):
            calls = list(idle_successful_tools.get() or [])
            names = ", ".join(dict.fromkeys(call["tool"] for call in calls))
            self.scheduler.record_idle_activity(
                account_id=account_id, event_id=event_id, event_number=number,
                model_id="system", started_at=started_at, finished_at=datetime.now(UTC),
                summary=(f"空闲活动未能生成总结（{failure or 'summary_not_recorded'}）；"
                         f"已保留成功工具记录：{names or '无'}。任务是否完成需按工具回执核实。"),
                outcome="failed", tool_calls=calls,
            )

    def agent_tools(self) -> list[ToolUnion]:
        return [agent_function_tool(self.record_idle_activity)]

    def finish(self, account_id: str, event_id: str) -> None:
        self.scheduler.update_idle_tool_calls(account_id, event_id, idle_successful_tools.get() or [])

    @staticmethod
    def bind(
        account_id: str, event_id: str, event_number: int, model_id: str, started_at: datetime
    ):
        return (
            _idle_scope.set((account_id, event_id, event_number, model_id, started_at)),
            idle_successful_tools.set([]),
        )

    @staticmethod
    def unbind(token) -> None:
        _idle_scope.reset(token[0])
        idle_successful_tools.reset(token[1])


    def adk_tools(self):
        """Legacy adapter; new runtimes consume agent_tools()."""
        from .frameworks.adk import to_adk
        return [to_adk(tool) for tool in self.agent_tools()]
