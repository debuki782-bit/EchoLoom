"""Small ADK tool facade for bounded cross-session operations."""

from __future__ import annotations

from collections.abc import Sequence

from .cross_session import CrossSessionService
from .runtime_contract import AgentTool as ToolUnion
from .tool_timing import agent_function_tool
from .wechat_tools import active_conversation, operation_id


class CrossSessionToolset:
    def __init__(self, service: CrossSessionService, account_id: str) -> None:
        self.service = service
        self.account_id = account_id

    def list_wechat_conversations(self, query: str = "", limit: int = 20) -> dict[str, object]:
        try:
            rows = self.service.list_conversations(
                account_id=self.account_id,
                query=query,
                limit=limit,
            )
        except ValueError as exc:
            return {"status": "rejected", "reason": str(exc)}
        return {"status": "completed", "conversations": rows}

    def query_wechat_messages(
        self,
        message_id: str = "",
        query: str = "",
        conversation_id: str = "",
        all_conversations: bool = False,
        sender_id: str = "",
        message_type: int = 0,
        start_at: str = "",
        end_at: str = "",
        before: int = 5,
        after: int = 5,
        limit: int = 0,
        byte_limit: int = 0,
    ) -> dict[str, object]:
        try:
            result = self.service.query_messages(
                account_id=self.account_id,
                current_conversation_id=active_conversation.get(),
                conversation_id=conversation_id or None,
                all_conversations=all_conversations,
                message_id=message_id,
                query=query,
                sender_id=sender_id,
                message_type=message_type,
                start_at=start_at,
                end_at=end_at,
                before=before,
                after=after,
                limit=limit,
                byte_limit=byte_limit or None,
            )
        except (ValueError, TypeError) as exc:
            return {"status": "rejected", "reason": str(exc)}
        return result

    async def delegate_to_wechat_conversation(
        self, conversation_id: str, task: str, return_result: bool = True
    ) -> dict[str, object]:
        source = active_conversation.get()
        if not source:
            return {"status": "unavailable", "reason": "no_active_source_conversation"}
        return await self.service.dispatch(
            account_id=self.account_id,
            source_conversation_id=source,
            target_conversation_id=conversation_id,
            task=task,
            return_result=return_result,
            operation_key=operation_id.get(),
        )

    def query_wechat_archive_sql(
        self, query: str, parameters: Sequence[str | int | float | None] = ()
    ) -> dict[str, object]:
        return self.service.query_sql(
            account_id=self.account_id,
            query=query,
            parameters=parameters,
        )

    def agent_tools(self) -> list[ToolUnion]:
        return [
            agent_function_tool(fn)
            for fn in (
                self.list_wechat_conversations,
                self.query_wechat_messages,
                self.delegate_to_wechat_conversation,
                self.query_wechat_archive_sql,
            )
        ]


    def adk_tools(self):
        """Legacy adapter; new runtimes consume agent_tools()."""
        from .frameworks.adk import to_adk
        return [to_adk(tool) for tool in self.agent_tools()]
