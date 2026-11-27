"""Pinned Hook text submission, with existing admission and receipt semantics."""
import hashlib
import logging
import random
import threading
import time
from collections.abc import Callable

import httpx

from ..connectors.base import SendResult
from ..contacts import is_contact_id
from ..models import ConversationKey, ReplyDraft

logger = logging.getLogger(__name__)


class SendPacer:
    """Thread-safe randomized cooldown between outbound WeChat Hook requests."""

    def __init__(
        self,
        min_interval_seconds: float = 0.55,
        max_interval_seconds: float = 1.35,
        *,
        rng: random.Random | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if min_interval_seconds < 0 or max_interval_seconds < min_interval_seconds:
            raise ValueError("send_cooldown_range bounds must satisfy 0 <= min <= max")
        self._min = float(min_interval_seconds)
        self._max = float(max_interval_seconds)
        self._rng = rng or random.SystemRandom()
        self._monotonic = monotonic
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next_allowed_at: float | None = None

    def wait_turn(self) -> float:
        """Reserve the next outbound slot and sleep only for any remaining cooldown."""
        if self._max <= 0:
            return 0.0
        with self._lock:
            now = self._monotonic()
            if self._next_allowed_at is None or now >= self._next_allowed_at:
                send_at = now
                wait = 0.0
            else:
                send_at = self._next_allowed_at
                wait = send_at - now
            cooldown = self._rng.uniform(self._min, self._max)
            self._next_allowed_at = send_at + cooldown
        if wait > 0:
            self._sleep(wait)
        return wait


class HookTextSender:
    """Single request to the fixed local POC endpoint, with no internal retry/redirect.

    Local approval, durable claim, privacy epoch, exact version/logged-in account
    verification and startup ownership remain responsibilities of the future
    worker; this class is not enabled by app.py.
    """

    def __init__(
        self, allowlist: frozenset[ConversationKey], *, enabled: bool = False,
        private_account_id: str | None = None,
        transport: httpx.BaseTransport | None = None,
        base_url: str = "http://127.0.0.1:30001",
        pacer: SendPacer | None = None,
    ) -> None:
        self.allowlist = set(allowlist)
        self.private_account_id = private_account_id
        self.enabled = enabled
        self._pacer = pacer if pacer is not None else SendPacer()
        self._client = httpx.Client(
            base_url=base_url, timeout=5.0, trust_env=False,
            follow_redirects=False, transport=transport,
        )

    def allow_conversation(self, key: ConversationKey) -> None:
        """Enable sends only after the Pilot has admitted this exact route."""
        self.allowlist.add(key)

    def submit(self, draft: ReplyDraft) -> SendResult:
        started_at = time.perf_counter()

        def finish(result: SendResult) -> SendResult:
            conversation_sha = hashlib.sha256(
                draft.key.conversation_id.encode("utf-8"),
            ).hexdigest()[:12]
            logger.info(
                "event=hook_send_finished operation=%s conversation_sha=%s status=%s "
                "reason=%s text_chars=%s elapsed_ms=%s",
                hashlib.sha256(draft.event_id.encode("utf-8")).hexdigest()[:12],
                conversation_sha, result.status,
                str(result.reason or "none")[:100], len(draft.text),
                int((time.perf_counter() - started_at) * 1000),
            )
            return result

        private_allowed = (
            self.private_account_id == draft.key.account_id
            and is_contact_id(draft.key.conversation_id)
        )
        if not self.enabled or (draft.key not in self.allowlist and not private_allowed):
            return finish(SendResult(
                "rejected", "sender_disabled_or_destination_not_allowed",
            ))
        if not draft.text or len(draft.text.encode("utf-8")) > 16384:
            return finish(SendResult("rejected", "empty_or_oversized_text"))
        if not draft.key.conversation_id or "\0" in draft.key.conversation_id + draft.text:
            return finish(SendResult("rejected", "invalid_destination_or_content"))
        try:
            self._pacer.wait_turn()
            response = self._client.post("/SendTextMsg", json={
                "wxidorgid": draft.key.conversation_id, "msg": draft.text,
            })
            if response.status_code != 200 or len(response.content) > 4096:
                return finish(SendResult("unknown", f"http_status_{response.status_code}"))
            result = response.json()
            if not isinstance(result, dict):
                return finish(SendResult("unknown", "hook_response_not_object"))
            code = result.get("ret")
            if type(code) is not int:
                return finish(SendResult("unknown", "hook_response_missing_integer_ret"))
            if code == 0 and result.get("queued") is True:
                return finish(SendResult("queued", "hook_queued_not_delivery_proof"))
            # These two codes precede the native send call in the pinned route.
            if code in (-1, -2) and result.get("queued") is not True:
                return finish(SendResult(
                    "rejected", f"hook_pre_send_rejection_ret_{code}",
                ))
            return finish(SendResult("unknown", f"unclassified_hook_ret_{code}"))
        except httpx.TimeoutException:
            return finish(SendResult("unknown", "hook_timeout_delivery_may_have_occurred"))
        except httpx.HTTPError:
            return finish(SendResult(
                "unknown", "hook_transport_error_delivery_may_have_occurred",
            ))
        except ValueError:
            return finish(SendResult("unknown", "hook_response_invalid_json"))
        return finish(SendResult("unknown", "hook_response_unclassified"))

    def close(self) -> None:
        self._client.close()
