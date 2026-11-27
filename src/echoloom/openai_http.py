"""Lightweight pure-httpx async client for OpenAI REST endpoints.

Replaces the external ``openai`` SDK package while keeping an identical minimal
surface (``responses.create``, ``responses.input_tokens.count``,
``embeddings.create``, ``images.generate``) and surfacing ``httpx`` HTTP errors
directly so budget accounting distinguishes pre-dispatch vs. indeterminate failures.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Self

import httpx

_DEFAULT_BASE_URL = "https://api.openai.com/v1"


def _to_namespace(value: Any) -> Any:
    if isinstance(value, dict):
        return SimpleNamespace(**{key: _to_namespace(val) for key, val in value.items()})
    if isinstance(value, list):
        return [_to_namespace(item) for item in value]
    return value


class _InputTokensResource:
    def __init__(self, owner: OpenAIHttpClient) -> None:
        self._owner = owner

    async def count(self, **kwargs: Any) -> Any:
        timeout = kwargs.pop("timeout", None)
        return await self._owner._post("/responses/input_tokens", kwargs, timeout=timeout)


class _ResponsesResource:
    def __init__(self, owner: OpenAIHttpClient) -> None:
        self._owner = owner
        self.input_tokens = _InputTokensResource(owner)

    async def create(self, **kwargs: Any) -> Any:
        timeout = kwargs.pop("timeout", None)
        return await self._owner._post("/responses", kwargs, timeout=timeout)


class _EmbeddingsResource:
    def __init__(self, owner: OpenAIHttpClient) -> None:
        self._owner = owner

    async def create(self, **kwargs: Any) -> Any:
        timeout = kwargs.pop("timeout", None)
        return await self._owner._post("/embeddings", kwargs, timeout=timeout)


class _ImagesResource:
    def __init__(self, owner: OpenAIHttpClient) -> None:
        self._owner = owner

    async def generate(self, **kwargs: Any) -> Any:
        timeout = kwargs.pop("timeout", None)
        return await self._owner._post("/images/generations", kwargs, timeout=timeout)


class OpenAIHttpClient:
    """Async REST client for OpenAI Responses, Embeddings, and Images endpoints."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = _DEFAULT_BASE_URL,
        timeout: float = 60.0,
        max_retries: int = 0,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        if max_retries != 0:
            raise ValueError("automatic SDK retries must stay disabled")
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = float(timeout)
        self._client = (
            http_client
            if http_client is not None
            else httpx.AsyncClient(base_url=self._base_url, timeout=self._timeout)
        )
        self.responses = _ResponsesResource(self)
        self.embeddings = _EmbeddingsResource(self)
        self.images = _ImagesResource(self)

    def _request_url(self, path: str) -> str:
        rel = path.lstrip("/")
        client_base = str(self._client.base_url).rstrip("/")
        if client_base:
            return f"{client_base}/{rel}"
        return f"{self._base_url}/{rel}"

    async def _post(
        self,
        path: str,
        payload: dict[str, Any],
        *,
        timeout: float | None = None,
    ) -> Any:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        effective_timeout = self._timeout if timeout is None else float(timeout)
        response = await self._client.post(
            self._request_url(path),
            json=payload,
            headers=headers,
            timeout=effective_timeout,
        )
        response.raise_for_status()
        return _to_namespace(response.json())

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()
