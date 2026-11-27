"""Per-call cost, token, and image-sub-budget boundary for ADK models."""

import asyncio
import base64
import hashlib
import inspect
import json
import logging
from collections import OrderedDict
from collections.abc import Callable
from datetime import datetime
from typing import Any
from uuid import uuid4

import google.genai.models as genai_models
import httpx
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.genai import types
from pydantic import BaseModel, PrivateAttr

from ..agent_profiles import (
    LUNA_ACTIVE_COMPRESSION_TOKENS,
    LUNA_IDLE_COMPRESSION_TOKENS,
)
from ..model_catalog import load_model_catalog
from ..ports import Clock
from ..providers import (
    ModelCallIndeterminate,
    _cache_write_rate,
    _cached_input_rate,
    _exception_chain,
    _http_status_code,
    _is_pre_dispatch_connect_error,
    _is_provider_circuit_status,
    _is_unbilled_reject_status,
    _microusd,
    _rates,
)
from ..usage import (
    DAILY_ACCOUNT_BUDGET_MICROUSD,
    IMAGE_GENERATION_DAILY_BUDGET_MICROUSD,
    IMAGE_READ_DAILY_BUDGET_MICROUSD,
    TEXT_MODEL_DAILY_BUDGET_MICROUSD,
    UsageLedger,
)

logger = logging.getLogger(__name__)
MAX_TEXT_CONTENT_BYTES = 32_768
MAX_SERIALIZED_REQUEST_BYTES = 131_072
MAX_MULTIMODAL_REQUEST_BYTES = 64_000_000
IMAGE_CONTEXT_BUDGET_MICROUSD = IMAGE_READ_DAILY_BUDGET_MICROUSD
IMAGE_CONTEXT_FEATURE = "adk_chat_image_context"
_TOKEN_COUNT_CACHE_MAX = 64
MODEL_TOKEN_LIMITS = load_model_catalog().token_limits_map()


def _instruction_text(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, types.Part):
        return value.text or ""
    if isinstance(value, types.Content):
        return "\n".join(
            part.text for part in value.parts or []
            if isinstance(part.text, str) and part.text
        )
    if isinstance(value, list):
        return "\n".join(text for item in value if (text := _instruction_text(item)))
    return ""


def _is_image_part(part: types.Part | types.FunctionResponsePart) -> bool:
    inline = getattr(part, "inline_data", None)
    file = getattr(part, "file_data", None)
    mime = ((getattr(inline, "mime_type", None) if inline else None)
            or (getattr(file, "mime_type", None) if file else None) or "")
    return mime.startswith("image/")


def _request_parts(request: LlmRequest) -> list[object]:
    parts: list[object] = []
    for content in request.contents:
        for part in content.parts or []:
            parts.append(part)
            function_response = getattr(part, "function_response", None)
            if function_response is not None:
                parts.extend(function_response.parts or [])
    system = request.config.system_instruction
    if isinstance(system, types.Content):
        parts.extend(system.parts or [])
    elif isinstance(system, types.Part):
        parts.append(system)
    elif isinstance(system, list):
        parts.extend(part for part in system if isinstance(part, types.Part))
    return parts


def _strip_request_images(request: LlmRequest, notice: str) -> int:
    """Strip image payloads from this request copy and annotate their tool results."""
    stripped = 0
    for content in request.contents:
        kept_content_parts: list[types.Part] = []
        direct_image_removed = False
        for part in content.parts or []:
            function_response = part.function_response
            if function_response is None:
                if _is_image_part(part):
                    stripped += 1
                    direct_image_removed = True
                    continue
                kept_content_parts.append(part)
                continue

            response_copy = function_response.model_copy(deep=True)
            kept_response_parts = []
            removed_from_response = False
            for response_part in response_copy.parts or []:
                if _is_image_part(response_part):
                    stripped += 1
                    removed_from_response = True
                else:
                    kept_response_parts.append(response_part)
            if removed_from_response:
                response_copy.parts = kept_response_parts or None
                response = dict(response_copy.response or {})
                response["image_access_notice"] = notice
                response_copy.response = response
                kept_content_parts.append(types.Part(function_response=response_copy))
            else:
                kept_content_parts.append(part)
        if direct_image_removed:
            kept_content_parts.append(types.Part.from_text(text=notice))
        content.parts = kept_content_parts
    return stripped


def _microusd_with_cached_input(
    input_tokens: int,
    cached_tokens: int,
    output_tokens: int,
    rates: tuple[int, int],
    cached_rate: int,
    *,
    cache_write_tokens: int = 0,
    cache_write_rate: int | None = None,
) -> int:
    if (
        cached_tokens < 0
        or cache_write_tokens < 0
        or cached_tokens + cache_write_tokens > input_tokens
    ):
        raise ValueError("cached input token count is invalid")
    write_rate = rates[0] if cache_write_rate is None else cache_write_rate
    uncached_tokens = input_tokens - cached_tokens - cache_write_tokens
    numerator = (
        uncached_tokens * rates[0]
        + cached_tokens * cached_rate
        + cache_write_tokens * write_rate
        + output_tokens * rates[1]
    )
    return (numerator + 999_999) // 1_000_000


from ..runtime_contract import (
    BudgetExhausted as AdkBudgetExhausted,
)
from ..runtime_contract import (
    ModelProviderUnavailable,
)
from ..runtime_scope import (  # noqa: F401 - legacy public re-exports
    _image_access_blocked,
    _profile_budgets,
    _route,
    block_image_access,
    current_model_usage_profile,
    current_model_usage_route,
    current_model_usage_scope_kwargs,
    image_access_blocked,
    model_usage_scope,
)


def _is_counter_transport_error(exc: BaseException, status: int | None) -> bool:
    """Return True if a token-count failure indicates upstream transport/circuit outage."""
    return (
        _is_pre_dispatch_connect_error(exc)
        or _is_provider_circuit_status(status)
        or any(
            isinstance(cur, (httpx.TransportError, TimeoutError, ConnectionError))
            or "Connection" in type(cur).__name__
            or "Timeout" in type(cur).__name__
            for cur in _exception_chain(exc)
        )
    )


def _serialized_request_bytes(request: LlmRequest) -> int:
    """Measure JSON payload bytes without failing on Python class response schemas."""
    config = getattr(request, "config", None)
    schema_obj = getattr(config, "response_schema", None) if config is not None else None
    if schema_obj is None:
        return len(request.model_dump_json(exclude={"tools_dict"}).encode())
    base_bytes = len(
        request.model_dump_json(exclude={"tools_dict": True, "config": {"response_schema": True}}).encode()
    )
    if hasattr(schema_obj, "model_dump_json") and not isinstance(schema_obj, type):
        schema_bytes = len(schema_obj.model_dump_json().encode())
    else:
        schema_payload = (
            schema_obj.model_json_schema()
            if hasattr(schema_obj, "model_json_schema")
            else str(schema_obj)
        )
        schema_bytes = len(
            json.dumps(
                schema_payload,
                ensure_ascii=False,
                separators=(",", ":"),
                default=str,
            ).encode()
        )
    return base_bytes + schema_bytes


def _count_tokens_config(request: LlmRequest) -> types.CountTokensConfig | None:
    """Extract system_instruction, tools, and structured output schema for official countTokens."""
    config = getattr(request, "config", None)
    if config is None:
        return None
    system_instruction = getattr(config, "system_instruction", None)
    tools = list(getattr(config, "tools", None) or [])
    response_schema = getattr(config, "response_schema", None)
    response_json_schema = getattr(config, "response_json_schema", None)
    response_mime_type = getattr(config, "response_mime_type", None)
    has_schema = any(
        value is not None for value in (response_schema, response_json_schema, response_mime_type)
    )
    gen_config = (
        types.GenerationConfig(
            response_schema=response_schema,
            response_json_schema=response_json_schema,
            response_mime_type=response_mime_type,
        )
        if has_schema
        else None
    )
    if system_instruction is None and not tools and gen_config is None:
        return None
    return types.CountTokensConfig(
        system_instruction=system_instruction,
        tools=tools or None,
        generation_config=gen_config,
    )


def _request_count_cache_key(model: str, request: LlmRequest) -> str:
    """Canonical SHA-256 digest of the token-counted request payload."""
    config = getattr(request, "config", None)
    sys_inst = getattr(config, "system_instruction", None)
    canonical = {
        "model": model,
        "contents": [
            content.model_dump(mode="json", exclude_none=True)
            for content in (request.contents or [])
        ],
        "system_instruction": (
            sys_inst.model_dump(mode="json", exclude_none=True)
            if isinstance(sys_inst, types.Content)
            else _instruction_text(sys_inst)
        ),
        "tools": [
            tool.model_dump(mode="json", exclude_none=True)
            for tool in (getattr(config, "tools", None) or [])
        ],
        "response_mime_type": getattr(config, "response_mime_type", None),
        "response_json_schema": getattr(config, "response_json_schema", None),
    }
    schema_obj = getattr(config, "response_schema", None)
    if schema_obj is not None:
        canonical["response_schema"] = (
            schema_obj.model_json_schema()
            if hasattr(schema_obj, "model_json_schema")
            else str(schema_obj)
        )
    encoded = json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _serialize_mldev_value(value: object) -> Any:
    """Convert Pydantic models, bytes, and datetimes into JSON-ready mldev dicts."""
    if isinstance(value, BaseModel):
        return _serialize_mldev_value(value.model_dump(exclude_none=True))
    if isinstance(value, bytes):
        return base64.urlsafe_b64encode(value).decode("ascii")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _serialize_mldev_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_serialize_mldev_value(item) for item in value]
    return value


async def _call_mldev_generate_content_count_tokens(
    api_client: genai_models.BaseApiClient,
    *,
    model: str,
    contents: list[types.Content],
    count_config: types.CountTokensConfig,
    http_options: types.HttpOptions | None = None,
) -> types.CountTokensResponse:
    """Invoke Gemini Developer API :countTokens with official generateContentRequest payload."""
    gen_cfg_fields: dict[str, object] = {}
    if count_config.system_instruction is not None:
        gen_cfg_fields["system_instruction"] = count_config.system_instruction
    if count_config.tools:
        gen_cfg_fields["tools"] = count_config.tools
    nested = count_config.generation_config
    if nested is not None:
        for attr in ("response_schema", "response_json_schema", "response_mime_type"):
            val = getattr(nested, attr, None)
            if val is not None:
                gen_cfg_fields[attr] = val
    gen_config = types.GenerateContentConfig(**gen_cfg_fields)  # type: ignore[arg-type]
    param_model = types._GenerateContentParameters(
        model=model,
        contents=contents,  # type: ignore[arg-type]
        config=gen_config,
    )
    request_dict = genai_models._GenerateContentParameters_to_mldev(
        api_client, param_model, None, param_model
    )
    url_dict = request_dict.pop("_url", None) or {}
    model_path = url_dict.get("model") or genai_models.t.t_model(api_client, model)
    request_dict.pop("_query", None)
    request_dict.pop("config", None)
    if not request_dict.get("generationConfig"):
        request_dict.pop("generationConfig", None)
    payload = _serialize_mldev_value(
        {"generateContentRequest": {"model": model_path, **request_dict}}
    )
    response = await api_client.async_request("post", f"{model_path}:countTokens", payload, http_options)
    response_dict = {} if not response.body else json.loads(response.body)
    parsed = genai_models._CountTokensResponse_from_mldev(
        response_dict,
        None,
        types._CountTokensParameters(model=model, contents=contents),
    )
    return types.CountTokensResponse._from_response(response=parsed, kwargs={})


def _resolve_real_genai_models(token_counter: object) -> genai_models.AsyncModels | None:
    """Return real un-mocked google.genai AsyncModels when token_counter wraps one."""
    provider = getattr(token_counter, "_gemini_client_provider", None)
    if callable(provider):
        client = provider()
        models_obj = getattr(getattr(client, "aio", None), "models", None)
        if (
            isinstance(models_obj, genai_models.AsyncModels)
            and isinstance(getattr(models_obj, "_api_client", None), genai_models.BaseApiClient)
            and "count_tokens" not in getattr(models_obj, "__dict__", {})
        ):
            return models_obj
        return None
    bound = getattr(token_counter, "__self__", None)
    if (
        isinstance(bound, genai_models.AsyncModels)
        and isinstance(getattr(bound, "_api_client", None), genai_models.BaseApiClient)
        and "count_tokens" not in getattr(bound, "__dict__", {})
    ):
        return bound
    return None


def _counter_accepts_config(token_counter: object) -> bool:
    try:
        params = inspect.signature(token_counter).parameters.values()  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return True
    return any(
        p.name == "config" or p.kind == inspect.Parameter.VAR_KEYWORD
        for p in params
    )


class BudgetedAdkModel(BaseLlm):
    _delegate: BaseLlm = PrivateAttr()
    _usage: UsageLedger = PrivateAttr()
    _clock: Clock = PrivateAttr()
    _budget: int = PrivateAttr()
    _input_limit: int = PrivateAttr()
    _output_limit: int = PrivateAttr()
    _idle_compression_tokens: int = PrivateAttr(default=LUNA_IDLE_COMPRESSION_TOKENS)
    _active_compression_tokens: int = PrivateAttr(default=LUNA_ACTIVE_COMPRESSION_TOKENS)
    _token_counter: Callable[..., Any] | None = PrivateAttr(default=None)
    _token_count_cache: OrderedDict[str, int] = PrivateAttr(default_factory=OrderedDict)
    _http_timeout_ms: int = PrivateAttr(default=20_000)
    _use_streaming: bool = PrivateAttr(default=False)
    _on_indeterminate: object | None = PrivateAttr(default=None)
    _budget_scope_id: str = PrivateAttr(default="default")
    _scope_budget: int = PrivateAttr(default=DAILY_ACCOUNT_BUDGET_MICROUSD)
    _text_budget: int = PrivateAttr(default=TEXT_MODEL_DAILY_BUDGET_MICROUSD)
    _image_read_budget: int = PrivateAttr(default=IMAGE_READ_DAILY_BUDGET_MICROUSD)
    _image_generation_budget: int = PrivateAttr(default=IMAGE_GENERATION_DAILY_BUDGET_MICROUSD)
    _account_text_budget: int = PrivateAttr(default=TEXT_MODEL_DAILY_BUDGET_MICROUSD)
    _account_image_read_budget: int = PrivateAttr(default=IMAGE_READ_DAILY_BUDGET_MICROUSD)

    def __init__(
        self,
        delegate: BaseLlm,
        usage: UsageLedger,
        clock: Clock,
        *,
        model_id: str,
        budget_microusd: int,
        max_input_tokens: int | None = None,
        max_output_tokens: int | None = None,
        token_counter=None,
        http_timeout_ms: int = 20_000,
        use_streaming: bool = False,
        on_indeterminate: Callable[[], object] | None = None,
        budget_scope_id: str = "default",
        scope_budget_microusd: int = DAILY_ACCOUNT_BUDGET_MICROUSD,
        text_budget_microusd: int = TEXT_MODEL_DAILY_BUDGET_MICROUSD,
        image_read_budget_microusd: int = IMAGE_READ_DAILY_BUDGET_MICROUSD,
        image_generation_budget_microusd: int = IMAGE_GENERATION_DAILY_BUDGET_MICROUSD,
        account_text_budget_microusd: int = TEXT_MODEL_DAILY_BUDGET_MICROUSD,
        account_image_read_budget_microusd: int = IMAGE_READ_DAILY_BUDGET_MICROUSD,
        idle_compression_tokens: int | None = None,
        active_compression_tokens: int | None = None,
    ):
        _rates(model_id, clock.now())
        catalog_spec = load_model_catalog().get(model_id)
        model_limits = (
            (catalog_spec.input_token_limit, catalog_spec.output_token_limit)
            if catalog_spec is not None
            else MODEL_TOKEN_LIMITS.get(model_id)
        )
        if model_limits is None or delegate.model != model_id:
            raise ValueError("invalid ADK model budget")
        input_limit = (
            model_limits[0] if max_input_tokens is None else max_input_tokens
        )
        output_limit = (
            model_limits[1] if max_output_tokens is None else max_output_tokens
        )
        resolved_idle_tokens = (
            idle_compression_tokens
            if idle_compression_tokens is not None
            else (
                LUNA_IDLE_COMPRESSION_TOKENS
                if input_limit > LUNA_ACTIVE_COMPRESSION_TOKENS
                else max(1, input_limit // 2)
            )
        )
        resolved_active_tokens = (
            active_compression_tokens
            if active_compression_tokens is not None
            else (
                LUNA_ACTIVE_COMPRESSION_TOKENS
                if input_limit > LUNA_ACTIVE_COMPRESSION_TOKENS
                else max(2, (input_limit * 3) // 4)
            )
        )
        if (
            budget_microusd < 0
            or input_limit < 1
            or output_limit < 1
            or not 0 < resolved_idle_tokens < resolved_active_tokens < input_limit
            or isinstance(http_timeout_ms, bool)
            or not isinstance(http_timeout_ms, int)
            or http_timeout_ms < 1
            or not budget_scope_id
            or min(scope_budget_microusd, text_budget_microusd,
                   image_read_budget_microusd, image_generation_budget_microusd) < 0
            or min(account_text_budget_microusd, account_image_read_budget_microusd) < 0
        ):
            raise ValueError("invalid ADK model budget")
        super().__init__(model=model_id)
        self._delegate, self._usage, self._clock = delegate, usage, clock
        self._budget = budget_microusd
        self._input_limit, self._output_limit = input_limit, output_limit
        self._idle_compression_tokens = resolved_idle_tokens
        self._active_compression_tokens = resolved_active_tokens
        self._token_counter = token_counter
        self._http_timeout_ms = http_timeout_ms
        self._use_streaming = bool(use_streaming)
        self._on_indeterminate = on_indeterminate
        self._budget_scope_id = budget_scope_id
        self._scope_budget = scope_budget_microusd
        self._text_budget = text_budget_microusd
        self._image_read_budget = image_read_budget_microusd
        self._image_generation_budget = image_generation_budget_microusd
        self._account_text_budget = account_text_budget_microusd
        self._account_image_read_budget = account_image_read_budget_microusd

    @staticmethod
    def _merge_stream_responses(responses: list[LlmResponse]) -> LlmResponse:
        """Merge aggregated non-partial streaming responses into one atomic response."""
        if len(responses) == 1:
            return responses[0]
        parts: list[types.Part] = []
        usage_metadata = None
        grounding_metadata = None
        citation_metadata = None
        for resp in responses:
            content = getattr(resp, "content", None)
            if content is not None and getattr(content, "parts", None):
                parts.extend(content.parts)
            if getattr(resp, "usage_metadata", None) is not None:
                usage_metadata = resp.usage_metadata
            if getattr(resp, "grounding_metadata", None) is not None:
                grounding_metadata = resp.grounding_metadata
            if getattr(resp, "citation_metadata", None) is not None:
                citation_metadata = resp.citation_metadata
        last = responses[-1]
        merged = last.model_copy(deep=True)
        merged.content = types.Content(role="model", parts=parts) if parts else last.content
        merged.usage_metadata = usage_metadata
        merged.grounding_metadata = grounding_metadata
        merged.citation_metadata = citation_metadata
        return merged

    def _notify_indeterminate(self) -> None:
        callback = self._on_indeterminate
        if not callable(callback):
            return
        try:
            callback()
        except Exception as exc:  # noqa: BLE001 - client refresh must stay best-effort
            logger.warning(
                "event=adk_indeterminate_callback_failed error_type=%s",
                type(exc).__name__,
            )

    @property
    def supports_token_counting(self) -> bool:
        return self._token_counter is not None or callable(
            getattr(self._delegate, "count_request_tokens", None)
        )

    @property
    def idle_compression_tokens(self) -> int:
        return self._idle_compression_tokens

    @property
    def active_compression_tokens(self) -> int:
        return self._active_compression_tokens

    @property
    def output_limit(self) -> int:
        return self._output_limit

    @output_limit.setter
    def output_limit(self, value: int) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError("output_limit must be positive")
        self._output_limit = value

    def _reservation_microusd(
        self,
        *,
        input_tokens: int | None,
        request_size: int,
        web_enabled: bool,
        search_enabled: bool,
        reserve_rates: tuple[int, int],
    ) -> int:
        search_allowance = 8 * 14_000 if search_enabled else 0
        estimated_input = (
            1_048_576
            if web_enabled
            else (input_tokens if input_tokens is not None else request_size + 4096)
        )
        return _microusd(estimated_input, self._output_limit, reserve_rates) + search_allowance

    async def count_request_tokens(self, request: LlmRequest) -> int | None:
        delegate_counter = getattr(self._delegate, "count_request_tokens", None)
        if callable(delegate_counter):
            try:
                async with asyncio.timeout(self._http_timeout_ms / 1000):
                    tokens = await delegate_counter(request)
            except Exception as exc:
                status = _http_status_code(exc)
                logger.error(
                    "event=adk_token_count_failed model=%s error_type=%s status_code=%s",
                    self.model,
                    type(exc).__name__,
                    status,
                )
                if _is_counter_transport_error(exc, status):
                    raise ModelProviderUnavailable(
                        f"ADK token counter unavailable: {type(exc).__name__}"
                        + (f" (HTTP {status})" if status is not None else "")
                    ) from exc
                raise
            if not isinstance(tokens, int) or tokens < 0:
                raise ValueError("model input token count unavailable")
            return tokens
        if self._token_counter is None:
            return None

        cache_key = _request_count_cache_key(self.model, request)
        cached = self._token_count_cache.get(cache_key)
        if cached is not None:
            self._token_count_cache.move_to_end(cache_key)
            return cached

        count_config = _count_tokens_config(request)
        token_counter = self._token_counter
        models_obj = _resolve_real_genai_models(token_counter)
        try:
            async with asyncio.timeout(self._http_timeout_ms / 1000):
                if models_obj is not None:
                    if getattr(models_obj._api_client, "vertexai", False) or count_config is None:
                        if count_config is not None:
                            counted = await models_obj.count_tokens(
                                model=self.model,
                                contents=request.contents,
                                config=count_config,
                            )
                        else:
                            counted = await models_obj.count_tokens(
                                model=self.model,
                                contents=request.contents,
                            )
                    else:
                        counted = await _call_mldev_generate_content_count_tokens(
                            models_obj._api_client,
                            model=self.model,
                            contents=list(request.contents),
                            count_config=count_config,
                            http_options=types.HttpOptions(timeout=self._http_timeout_ms),
                        )
                elif token_counter is None:
                    raise ModelProviderUnavailable("ADK token counter unavailable")
                elif count_config is None or _counter_accepts_config(token_counter):
                    if count_config is not None:
                        counted = await token_counter(
                            model=self.model,
                            contents=request.contents,
                            config=count_config,
                        )
                    else:
                        counted = await token_counter(
                            model=self.model,
                            contents=request.contents,
                        )
                else:
                    # Compatibility fallback for minimal test stubs accepting only (model, contents).
                    count_contents = list(request.contents)
                    system_text = _instruction_text(request.config.system_instruction)
                    if system_text:
                        count_contents.append(types.Content(
                            role="user",
                            parts=[types.Part.from_text(text=system_text)],
                        ))
                    tool_text = "\n".join(
                        tool.model_dump_json(exclude_none=True)
                        for tool in (request.config.tools or [])
                        if isinstance(tool, BaseModel)
                    )
                    if tool_text:
                        count_contents.append(types.Content(
                            role="user",
                            parts=[types.Part.from_text(text=tool_text)],
                        ))
                    counted = await token_counter(
                        model=self.model,
                        contents=count_contents,
                    )
        except Exception as exc:
            status = _http_status_code(exc)
            logger.error(
                "event=adk_token_count_failed model=%s error_type=%s status_code=%s",
                self.model,
                type(exc).__name__,
                status,
            )
            if _is_counter_transport_error(exc, status):
                raise ModelProviderUnavailable(
                    f"ADK token counter unavailable: {type(exc).__name__}"
                    + (f" (HTTP {status})" if status is not None else "")
                ) from exc
            raise
        tokens = counted.total_tokens
        if not isinstance(tokens, int) or tokens < 0:
            raise ValueError("model input token count unavailable")
        self._token_count_cache[cache_key] = tokens
        if len(self._token_count_cache) > _TOKEN_COUNT_CACHE_MAX:
            self._token_count_cache.popitem(last=False)
        return tokens

    async def generate_content_async(self, llm_request: LlmRequest, stream: bool = False):
        route = _route.get()
        if route is None or stream:
            raise ValueError("budgeted model requires bound route and non-streaming mode")
        request = llm_request.model_copy(deep=True)
        request.model = self.model
        request.config.temperature = 1.0
        request.config.max_output_tokens = self._output_limit
        request.config.candidate_count = 1
        if self.model.startswith("gemini-") and getattr(request.config, "response_schema", None) is None:
            request.config.response_modalities = ["TEXT"]
        catalog_spec = load_model_catalog().get(self.model)
        thinking_level = catalog_spec.thinking_level if catalog_spec is not None else None
        if thinking_level:
            request.config.thinking_config = types.ThinkingConfig(
                thinking_level=types.ThinkingLevel(thinking_level)
            )
        request.config.http_options = types.HttpOptions(
            timeout=self._http_timeout_ms,
            retry_options=types.HttpRetryOptions(attempts=1),
        )
        if request.config.cached_content or any(
            not isinstance(tool, types.Tool)
            or set(tool.model_dump(exclude_none=True))
            - {"function_declarations", "google_search", "url_context"}
            for tool in request.config.tools or []
        ):
            raise ValueError("cached content or unsupported built-in tool")

        def sizes(current: LlmRequest) -> tuple[list[object], bool, bool, int, int]:
            all_parts = _request_parts(current)
            images = [
                part
                for part in all_parts
                if isinstance(part, (types.Part, types.FunctionResponsePart))
                and _is_image_part(part)
            ]
            unsupported = any(
                (getattr(part, "inline_data", None) or getattr(part, "file_data", None))
                and not (
                    isinstance(part, (types.Part, types.FunctionResponsePart))
                    and _is_image_part(part)
                )
                for part in all_parts
            )
            text_bytes = sum(
                len((getattr(part, "text", None) or "").encode("utf-8"))
                for part in all_parts
            )
            serialized_bytes = _serialized_request_bytes(current)
            return all_parts, bool(images), unsupported, text_bytes, serialized_bytes

        async def count_tokens(current: LlmRequest) -> int | None:
            return await self.count_request_tokens(current)

        _, has_images, unsupported_media, text_size, request_size = sizes(request)
        if unsupported_media:
            raise ValueError("audio and other media pricing is not configured")
        if has_images and self._token_counter is None and not callable(
            getattr(self._delegate, "count_request_tokens", None)
        ):
            raise ValueError("image requests require the model token counter")

        notice = ""
        if has_images and request_size > MAX_MULTIMODAL_REQUEST_BYTES:
            notice = (
                "图片像素超过本次请求的传输上限，已保留在会话记录；"
                "本轮无法查看，请勿重复调用读图工具。"
            )
            _strip_request_images(request, notice)
            block_image_access()
            logger.warning("event=adk_image_payload_stripped reason=request_size_limit")
            _, has_images, unsupported_media, text_size, request_size = sizes(request)

        input_tokens = await count_tokens(request)
        if has_images and input_tokens is not None and input_tokens > self._input_limit:
            notice = (
                "图片会使本次输入超过模型档位上限，已保留在会话记录；"
                "本轮无法查看，请勿重复调用读图工具。"
            )
            _strip_request_images(request, notice)
            block_image_access()
            logger.warning("event=adk_image_payload_stripped reason=input_token_limit")
            _, has_images, unsupported_media, text_size, request_size = sizes(request)
            input_tokens = await count_tokens(request)

        hard_byte_limit = (
            (MAX_TEXT_CONTENT_BYTES, MAX_SERIALIZED_REQUEST_BYTES)
            if (
                self._token_counter is None
                and not callable(getattr(self._delegate, "count_request_tokens", None))
            )
            else (1_000_000, 1_500_000)
        )
        if has_images:
            hard_byte_limit = (1_000_000, MAX_MULTIMODAL_REQUEST_BYTES)
        if (
            text_size > hard_byte_limit[0]
            or request_size > hard_byte_limit[1]
            or (input_tokens is not None and input_tokens > self._input_limit)
        ):
            logger.warning(
                "event=adk_request_size_rejected text_bytes=%s request_bytes=%s "
                "input_tokens=%s input_token_limit=%s text_limit=%s request_limit=%s",
                text_size,
                request_size,
                input_tokens,
                self._input_limit,
                hard_byte_limit[0],
                hard_byte_limit[1],
            )
            raise ValueError(
                f"ADK input exceeds budget ({input_tokens or 'unknown'} tokens; "
                f"limit {self._input_limit}; {text_size}/{request_size} bytes)"
            )
        logger.info(
            "event=adk_request_budget model=%s text_bytes=%s request_bytes=%s "
            "input_tokens=%s input_token_limit=%s image_payload_stripped=%s",
            self.model,
            text_size,
            request_size,
            input_tokens,
            self._input_limit,
            bool(notice),
        )
        now_iso = self._clock.now()
        rates = _rates(self.model, now_iso)
        write_rate = _cache_write_rate(self.model, now_iso)
        reserve_rates = (write_rate, rates[1])
        tools_list = request.config.tools or []
        search_enabled = any(
            isinstance(tool, types.Tool) and tool.google_search is not None
            for tool in tools_list
        )
        web_enabled = search_enabled or any(
            isinstance(tool, types.Tool) and tool.url_context is not None
            for tool in tools_list
        )
        # Remote page/tool inputs aren't included in countTokens. Reserve the full
        # model context, plus an eight-query allowance (Google has no query cap).
        maximum = self._reservation_microusd(
            input_tokens=input_tokens,
            request_size=request_size,
            web_enabled=web_enabled,
            search_enabled=search_enabled,
            reserve_rates=reserve_rates,
        )
        request_id = "adk:" + uuid4().hex
        feature = IMAGE_CONTEXT_FEATURE if has_images else "adk_chat"
        reserved = self._usage.reserve(
            request_id=request_id,
            account_id=route[0],
            conversation_id=route[1],
            feature=feature,
            model_id=self.model,
            maximum_microusd=maximum,
            budget_microusd=self._budget,
            feature_budget_microusd=(self._image_read_budget if has_images
                                     else self._text_budget),
            budget_group="image_read" if has_images else "text_model",
            budget_scope_id=self._budget_scope_id,
            scope_budget_microusd=self._scope_budget,
            account_feature_budget_microusd=(self._account_image_read_budget if has_images
                                             else self._account_text_budget),
        )
        if reserved.status in (
            "feature_budget_exhausted", "account_feature_budget_exhausted",
        ) and has_images:
            notice = (
                "今日读图预算已用完，图片仍保存在会话记录中；"
                "本轮不发送图片像素，请勿重复调用读图工具。"
            )
            _strip_request_images(request, notice)
            block_image_access()
            logger.warning("event=adk_image_payload_stripped reason=daily_image_budget")
            _, has_images, unsupported_media, text_size, request_size = sizes(request)
            input_tokens = await count_tokens(request)
            if input_tokens is None or input_tokens > self._input_limit:
                raise ValueError("text-only request exceeds model input budget")
            if text_size > 1_000_000 or request_size > 1_500_000:
                raise ValueError("text-only request exceeds hard request size limit")
            maximum = self._reservation_microusd(
                input_tokens=input_tokens,
                request_size=request_size,
                web_enabled=web_enabled,
                search_enabled=search_enabled,
                reserve_rates=reserve_rates,
            )
            feature = "adk_chat"
            reserved = self._usage.reserve(
                request_id=request_id,
                account_id=route[0],
                conversation_id=route[1],
                feature=feature,
                model_id=self.model,
                maximum_microusd=maximum,
                budget_microusd=self._budget,
                feature_budget_microusd=self._text_budget,
                budget_group="text_model",
                budget_scope_id=self._budget_scope_id,
                scope_budget_microusd=self._scope_budget,
                account_feature_budget_microusd=self._account_text_budget,
            )
        if reserved.status != "reserved":
            if reserved.status in (
                "budget_exhausted", "scope_budget_exhausted",
                "feature_budget_exhausted", "account_feature_budget_exhausted",
            ):
                raise AdkBudgetExhausted(
                    f"ADK {reserved.status.replace('_', ' ')}; no model call issued; "
                    "increase the selected profile cap"
                )
            raise RuntimeError(f"ADK usage reservation refused: {reserved.status}")
        response = None
        saw_partial = False
        try:
            timeout_seconds = self._http_timeout_ms / 1000 + (30 if self._use_streaming else 5)
            async with asyncio.timeout(timeout_seconds):
                if self._use_streaming:
                    finals = []
                    async for item in self._delegate.generate_content_async(request, stream=True):
                        if getattr(item, "partial", False):
                            saw_partial = True
                            continue
                        finals.append(item)
                    if finals:
                        response = self._merge_stream_responses(finals)
                else:
                    async for item in self._delegate.generate_content_async(request, stream=False):
                        if response is not None or item.partial:
                            raise ValueError("unexpected partial or multiple responses")
                        response = item
        except Exception as exc:  # noqa: BLE001 - post-call failures retain the reservation
            if response is None and not saw_partial and _is_pre_dispatch_connect_error(exc):
                self._usage.release(request_id)
                logger.warning(
                    "event=adk_provider_unavailable model=%s error_type=%s reservation_released=true",
                    self.model,
                    type(exc).__name__,
                )
                raise ModelProviderUnavailable(
                    "ADK provider unreachable before dispatch; reservation released"
                ) from None
            status = _http_status_code(exc) if (response is None and not saw_partial) else None
            if response is None and not saw_partial and _is_provider_circuit_status(status):
                self._usage.release(request_id)
                logger.warning(
                    "event=adk_provider_rejected model=%s error_type=%s status_code=%s "
                    "reservation_released=true circuit_trip=true",
                    self.model,
                    type(exc).__name__,
                    status,
                )
                raise ModelProviderUnavailable(
                    f"ADK provider rejected request (HTTP {status}); reservation released"
                ) from None
            if response is None and not saw_partial and _is_unbilled_reject_status(status):
                self._usage.release(request_id)
                logger.warning(
                    "event=adk_request_rejected model=%s error_type=%s status_code=%s "
                    "reservation_released=true circuit_trip=false",
                    self.model,
                    type(exc).__name__,
                    status,
                )
                raise ModelCallIndeterminate(
                    f"ADK request rejected (HTTP {status}); reservation released"
                ) from None
            logger.warning(
                "event=adk_call_indeterminate model=%s feature=%s error_type=%s status_code=%s "
                "had_response=%s saw_partial=%s reservation_retained=true request_id=%s",
                self.model,
                feature,
                type(exc).__name__,
                status,
                response is not None,
                saw_partial,
                request_id,
            )
            self._notify_indeterminate()
            raise ModelCallIndeterminate("ADK result unknown; reservation retained") from None
        metadata = response.usage_metadata if response else None
        if (
            metadata is None
            or metadata.prompt_token_count is None
            or metadata.total_token_count is None
        ):
            logger.warning(
                "event=adk_call_indeterminate model=%s feature=%s reason=missing_usage "
                "had_response=%s saw_partial=%s reservation_retained=true request_id=%s",
                self.model,
                feature,
                response is not None,
                saw_partial,
                request_id,
            )
            raise ModelCallIndeterminate("ADK usage missing; reservation retained")
        prompt, total = metadata.prompt_token_count, metadata.total_token_count
        if prompt < 0 or total < prompt:
            logger.warning(
                "event=adk_call_indeterminate model=%s feature=%s reason=invalid_usage "
                "reservation_retained=true request_id=%s",
                self.model,
                feature,
                request_id,
            )
            raise ModelCallIndeterminate("ADK usage invalid; reservation retained")
        cached_tokens = int(getattr(metadata, "cached_content_token_count", 0) or 0)
        cache_write_tokens = int(
            (getattr(response, "custom_metadata", None) or {}).get("openai_cache_write_tokens", 0) or 0
        )
        if cached_tokens < 0 or cache_write_tokens < 0 or cached_tokens + cache_write_tokens > prompt:
            logger.warning(
                "event=adk_cached_token_metadata_invalid prompt_tokens=%d cached_tokens=%d "
                "cache_write_tokens=%d pricing_cached_tokens_as_zero=true",
                prompt,
                cached_tokens,
                cache_write_tokens,
            )
            cached_tokens = 0
            cache_write_tokens = 0
        tool_input = int(getattr(metadata, "tool_use_prompt_token_count", 0) or 0)
        if tool_input < 0 or total < prompt + tool_input:
            logger.warning(
                "event=adk_call_indeterminate model=%s feature=%s reason=invalid_tool_usage "
                "reservation_retained=true request_id=%s",
                self.model,
                feature,
                request_id,
            )
            raise ModelCallIndeterminate("ADK tool input usage invalid; reservation retained")
        grounding = getattr(response, "grounding_metadata", None)
        search_queries = len(getattr(grounding, "web_search_queries", None) or [])
        actual = _microusd_with_cached_input(
            prompt,
            cached_tokens,
            total - prompt - tool_input,
            rates,
            _cached_input_rate(self.model, now_iso),
            cache_write_tokens=cache_write_tokens,
            cache_write_rate=write_rate,
        ) + _microusd(tool_input, 0, rates) + search_queries * 14_000
        state = self._usage.settle(request_id, actual)
        if state == "over_reservation":
            logger.warning("event=adk_usage_over_reservation actual_microusd=%s", actual)
        elif state != "settled":
            logger.warning(
                "event=adk_call_indeterminate model=%s feature=%s reason=settle_state_%s "
                "reservation_retained=true request_id=%s",
                self.model,
                feature,
                state,
                request_id,
            )
            raise ModelCallIndeterminate("ADK usage exceeded reservation or settlement failed")
        yield response
