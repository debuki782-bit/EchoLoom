"""Pinned Hook contact lookup and its bounded identity cache."""
import threading
import time

import httpx

from ..contacts import ContactIdentity, _optional_label, is_contact_id, is_room_id


class HookContactResolver:
    """Bounded, in-memory resolver using only the local Hook's exact-ID route."""

    def __init__(self, base_url: str = "http://127.0.0.1:30001", *,
                 timeout_seconds: float = 3.0, positive_ttl_seconds: float = 30.0,
                 negative_ttl_seconds: float = 5.0,
                 transport: httpx.BaseTransport | None = None):
        if not base_url.startswith("http://127.0.0.1:") or timeout_seconds <= 0:
            raise ValueError("contact lookup must use a positive-timeout loopback URL")
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds
        self._positive_ttl = positive_ttl_seconds
        self._negative_ttl = negative_ttl_seconds
        self._transport = transport
        self._lock = threading.Lock()
        self._cache: dict[str, tuple[float, ContactIdentity | None]] = {}

    def resolve(self, user_id: str) -> ContactIdentity | None:
        """Return a label only when Hook confirms an exact Contact.UserName match."""
        if not (is_contact_id(user_id) or is_room_id(user_id)):
            return None
        now = time.monotonic()
        with self._lock:
            cached = self._cache.get(user_id)
            if cached and cached[0] > now:
                return cached[1]

        identity = self._query(user_id)
        ttl = self._positive_ttl if identity is not None else self._negative_ttl
        with self._lock:
            self._cache[user_id] = (time.monotonic() + ttl, identity)
        return identity

    def _query(self, user_id: str) -> ContactIdentity | None:
        try:
            with httpx.Client(timeout=self._timeout, trust_env=False,
                               transport=self._transport) as client:
                response = client.post(f"{self._base_url}/GetContact",
                                       json={"wxid": user_id})
                if response.status_code != 200:
                    return None
                data = response.json()
        except (httpx.HTTPError, ValueError, TypeError):
            return None
        if (not isinstance(data, dict) or data.get("status") != 0
                or data.get("contact_found") is not True
                or data.get("UserName") != user_id):
            return None
        nickname = _optional_label(data.get("NickName"))
        return ContactIdentity(
            display_name=nickname or user_id, user_name=user_id,
            nickname=nickname,
        )
