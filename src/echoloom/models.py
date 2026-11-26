"""Transport-independent domain data for the M2 mock slice."""

from dataclasses import dataclass
from enum import StrEnum


class DeliveryState(StrEnum):
    DRAFT = "draft"
    MOCK_SENT = "mock_sent"
    DROPPED = "dropped"


@dataclass(frozen=True)
class ConversationKey:
    account_id: str
    conversation_id: str


@dataclass(frozen=True)
class GroupTrust:
    """Evidence produced by a trusted adapter, never copied from chat JSON."""

    origin_authenticated: bool
    sender_native: bool
    membership_verified: bool
    source_event_id_stable: bool

    @property
    def permits_processing(self) -> bool:
        return all((
            self.origin_authenticated,
            self.sender_native,
            self.membership_verified,
            self.source_event_id_stable,
        ))


@dataclass(frozen=True)
class InboundEvent:
    event_id: str
    account_id: str
    conversation_id: str
    sender_id: str
    text: str
    is_group: bool = False
    message_type: str = "text"
    group_trust: GroupTrust | None = None

    @property
    def key(self) -> ConversationKey:
        return ConversationKey(self.account_id, self.conversation_id)


@dataclass(frozen=True)
class ReplyDraft:
    event_id: str
    key: ConversationKey
    text: str
    privacy_epoch: int


@dataclass(frozen=True)
class ProcessResult:
    status: str
    draft: ReplyDraft | None = None
