"""Initial plugin boundary: one server-owned read-only status tool."""

from dataclasses import dataclass

from .runtime_contract import AgentTool as ToolUnion

from .tool_timing import agent_function_tool


@dataclass(frozen=True)
class AgentToolStatus:
    """Public capability snapshot supplied by the application, never chat JSON."""

    mode: str = "offline_mock"
    automatic_send_enabled: bool = False
    supported_sends: tuple[str, ...] = ()


class ReadOnlyToolset:
    def __init__(self, status: AgentToolStatus) -> None:
        self._status = status

    def read_gateway_status(self) -> dict[str, object]:
        return {
            "mode": self._status.mode,
            "automatic_send_enabled": self._status.automatic_send_enabled,
            "supported_sends": list(self._status.supported_sends),
        }

    def agent_tools(self) -> list[ToolUnion]:
        # Explicit catalog: no module/path/HTTP/shell lookup from model arguments.
        return [agent_function_tool(self.read_gateway_status)]


    def adk_tools(self):
        """Legacy adapter; new runtimes consume agent_tools()."""
        from .frameworks.adk import to_adk
        return [to_adk(tool) for tool in self.agent_tools()]
