"""Legacy ADK entry point, retained for offline regressions and rollback."""
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.genai import types
from pydantic import PrivateAttr
from ..session_runtime import ScopedAgentRuntime, _estimated_event_bytes, session_keys  # noqa: F401
from .adk import AdkFramework

class DeterministicLlm(BaseLlm):
    """No credentials, network, tools, or cost."""

    _requests: list[LlmRequest] = PrivateAttr(default_factory=list)

    def __init__(self) -> None:
        super().__init__(model="offline-fake")

    @property
    def requests(self) -> list[LlmRequest]:
        return self._requests

    async def generate_content_async(self, llm_request: LlmRequest, stream: bool = False):
        self._requests.append(llm_request)
        yield LlmResponse(
            content=types.Content(
                role="model",
                parts=[types.Part.from_text(text="[MOCK ADK] 已收到消息。")],
            )
        )



class ScopedAdkRunner(ScopedAgentRuntime):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("model", DeterministicLlm())
        kwargs["framework"] = AdkFramework()
        super().__init__(*args, **kwargs)
