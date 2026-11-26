"""Application-owned Agent boundary. No provider or Agent SDK imports."""
from dataclasses import dataclass
from typing import Any, Callable, Protocol
from types import SimpleNamespace

from pydantic import BaseModel, ConfigDict


class Part(BaseModel):
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)
    text: str | None = None
    thought: bool = False
    inline_data: Any = None
    file_data: Any = None
    function_call: Any = None
    function_response: Any = None

    @classmethod
    def from_text(cls, *, text: str):
        return cls(text=text)

    @classmethod
    def from_bytes(cls, *, data: bytes, mime_type: str):
        return cls(inline_data=SimpleNamespace(data=data, mime_type=mime_type))


class Content(BaseModel):
    role: str
    parts: list[Any] = []


@dataclass
class RunOptions:
    max_llm_calls: int = 12
    show_call_budget: bool = False


@dataclass
class ToolContext:
    ended: bool = False

    def end_turn(self):
        self.ended = True


@dataclass
class AgentTool:
    function: Callable
    name: str
    description: str


class BudgetExhausted(RuntimeError):
    pass


class ModelCallsLimitExceeded(RuntimeError):
    pass


class ModelCallIndeterminate(RuntimeError):
    """A provider request may have executed or been billed; do not assume failure."""


class ModelProviderUnavailable(RuntimeError):
    """Transport connection failed before any billable provider request was sent."""


class AgentRuntime(Protocol):
    async def reply(self, window) -> str: ...
    async def register_conversation(self, account_id: str, conversation_id: str) -> None: ...
    async def record_message_activity(self, account_id: str, conversation_id: str) -> None: ...
    async def summarize_idle_sessions(self) -> int: ...
    async def close(self) -> None: ...


class AgentFramework(Protocol):
    def default_model(self): ...
    def sessions(self, database_path=None): ...
    def runner(self, *, model, sessions, instruction, tools=(), callback=None,
               app_name="wechat_agent_mock", threshold=80_000): ...
