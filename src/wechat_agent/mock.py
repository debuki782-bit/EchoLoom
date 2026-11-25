"""Explicit fake dependencies; these never contact WeChat or a cloud API."""

from dataclasses import dataclass, field
from datetime import UTC, datetime

from .models import InboundEvent, ReplyDraft


@dataclass
class MockTransport:
    sent: list[ReplyDraft] = field(default_factory=list)

    def send(self, draft: ReplyDraft) -> bool:
        self.sent.append(draft)
        return True


class MockProvider:
    def reply(self, event: InboundEvent) -> str:
        return f"[MOCK] 收到事件 {event.event_id}；未调用真实模型。"


class SystemClock:
    def now(self) -> str:
        return datetime.now(UTC).isoformat()
