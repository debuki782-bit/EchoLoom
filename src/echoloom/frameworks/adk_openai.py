"""Google ADK BaseLlm adapter for OpenAI Responses API models."""

from __future__ import annotations

import base64
from collections import OrderedDict
import copy
import hashlib
import json
from typing import Any

from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.genai import types
from pydantic import BaseModel, PrivateAttr


def _instruction_text(value: object) -> str | None:
    if isinstance(value, str):
        return value or None
    if isinstance(value, types.Part):
        return value.text or None
    if isinstance(value, types.Content):
        text = "\n".join(
            part.text
            for part in value.parts or []
            if isinstance(part.text, str) and part.text
        )
        return text or None
    if isinstance(value, list):
        lines = [item_text for item in value if (item_text := _instruction_text(item))]
        return "\n".join(lines) if lines else None
    return None


def _strict_json_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Ensure object nodes satisfy OpenAI Responses strict json_schema rules."""
    out = copy.deepcopy(schema)

    def walk(node: object) -> None:
        if isinstance(node, dict):
            if node.get("type") == "object" or "properties" in node:
                props = node.get("properties")
                if isinstance(props, dict):
                    node["additionalProperties"] = False
                    node["required"] = list(props.keys())
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(out)
    return out


def _convert_tools(tools: list[types.Tool] | None) -> list[dict[str, Any]]:
    wire: list[dict[str, Any]] = []
    for tool in tools or ():
        for decl in getattr(tool, "function_declarations", None) or ():
            raw_schema: dict[str, Any] | None = getattr(decl, "parameters_json_schema", None)
            if raw_schema is None and getattr(decl, "parameters", None) is not None:
                raw_schema = decl.parameters.model_dump(exclude_none=True)
            parameters = (
                dict(raw_schema)
                if isinstance(raw_schema, dict)
                else {"type": "object", "properties": {}}
            )
            parameters.setdefault("type", "object")
            parameters.setdefault("properties", {})
            parameters.setdefault("additionalProperties", False)
            wire.append(
                {
                    "type": "function",
                    "name": decl.name,
                    "description": decl.description or "",
                    "parameters": parameters,
                    "strict": False,
                }
            )
    return wire


def _convert_text_format(config: types.GenerateContentConfig | None) -> dict[str, Any] | None:
    if config is None:
        return None
    schema_obj = getattr(config, "response_schema", None)
    json_schema = getattr(config, "response_json_schema", None)
    if schema_obj is not None:
        if isinstance(schema_obj, type) and issubclass(schema_obj, BaseModel):
            name = schema_obj.__name__
            schema = _strict_json_schema(schema_obj.model_json_schema())
            return {"format": {"type": "json_schema", "name": name, "schema": schema, "strict": True}}
        if isinstance(schema_obj, dict):
            return {
                "format": {
                    "type": "json_schema",
                    "name": "StructuredOutput",
                    "schema": _strict_json_schema(schema_obj),
                    "strict": True,
                }
            }
    if isinstance(json_schema, dict):
        return {
            "format": {
                "type": "json_schema",
                "name": "StructuredOutput",
                "schema": _strict_json_schema(json_schema),
                "strict": True,
            }
        }
    return None


def _encode_function_output(fr: types.FunctionResponse) -> str | list[dict[str, Any]]:
    payload = fr.response
    text = (
        payload
        if isinstance(payload, str)
        else json.dumps(payload if payload is not None else {}, ensure_ascii=False, default=str)
    )
    images: list[dict[str, Any]] = []
    for part in getattr(fr, "parts", None) or ():
        inline = getattr(part, "inline_data", None)
        if inline is not None and getattr(inline, "data", None) is not None:
            mime = getattr(inline, "mime_type", None) or "image/png"
            encoded = base64.b64encode(inline.data).decode("ascii")
            images.append(
                {
                    "type": "input_image",
                    "image_url": f"data:{mime};base64,{encoded}",
                    "detail": "auto",
                }
            )
    if images:
        return [{"type": "input_text", "text": text}, *images]
    return text


def _convert_contents(contents: list[types.Content]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for index, content in enumerate(contents):
        role = "assistant" if getattr(content, "role", None) == "model" else "user"
        message_blocks: list[dict[str, Any]] = []

        def flush_message() -> None:
            if message_blocks:
                items.append({"role": role, "content": list(message_blocks)})
                message_blocks.clear()

        for part_idx, part in enumerate(getattr(content, "parts", None) or ()):
            if getattr(part, "thought", False):
                continue
            fc = getattr(part, "function_call", None)
            if fc is not None:
                flush_message()
                call_id = getattr(fc, "id", None) or f"adk_call_{index}_{part_idx}"
                items.append(
                    {
                        "type": "function_call",
                        "call_id": call_id,
                        "name": fc.name,
                        "arguments": json.dumps(fc.args or {}, ensure_ascii=False),
                    }
                )
                continue
            fr = getattr(part, "function_response", None)
            if fr is not None:
                flush_message()
                call_id = getattr(fr, "id", None) or f"adk_call_{index}_{part_idx}"
                items.append(
                    {
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": _encode_function_output(fr),
                    }
                )
                continue
            inline = getattr(part, "inline_data", None)
            if inline is not None and getattr(inline, "data", None) is not None:
                mime = getattr(inline, "mime_type", None) or "image/png"
                if mime.startswith("image/"):
                    encoded = base64.b64encode(inline.data).decode("ascii")
                    message_blocks.append(
                        {
                            "type": "input_image",
                            "image_url": f"data:{mime};base64,{encoded}",
                            "detail": "auto",
                        }
                    )
                    continue
            text = getattr(part, "text", None)
            if isinstance(text, str) and text:
                block_type = "output_text" if role == "assistant" else "input_text"
                message_blocks.append({"type": block_type, "text": text})
        flush_message()
    return items


_TOKEN_COUNT_CACHE_MAX = 64


class OpenAIAdkModel(BaseLlm):
    """Run OpenAI Responses API models natively inside Google ADK."""

    _client: Any = PrivateAttr()
    _timeout_seconds: float = PrivateAttr(default=60.0)
    _token_count_cache: OrderedDict[str, int] = PrivateAttr(default_factory=OrderedDict)

    def __init__(
        self,
        model: str,
        client: Any,
        *,
        timeout_seconds: float = 60.0,
    ) -> None:
        super().__init__(model=model)
        self._client = client
        self._timeout_seconds = float(timeout_seconds)
        self._token_count_cache = OrderedDict()

    def _build_payload(self, request: LlmRequest) -> dict[str, Any]:
        instructions = _instruction_text(getattr(request.config, "system_instruction", None))
        input_items = _convert_contents(list(request.contents or ()))
        tools = _convert_tools(getattr(request.config, "tools", None))
        payload: dict[str, Any] = {
            "model": self.model,
            "input": input_items,
            "parallel_tool_calls": False,
            "reasoning": {"effort": "none"},
        }
        if instructions:
            payload["instructions"] = instructions
        if tools:
            payload["tools"] = tools
        text_format = _convert_text_format(request.config)
        if text_format is not None:
            payload["text"] = text_format
        return payload

    async def count_request_tokens(self, request: LlmRequest) -> int:
        """Count the complete OpenAI Responses payload once and cache identical prefights."""
        payload = self._build_payload(request)
        cache_key = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        cached = self._token_count_cache.get(cache_key)
        if cached is not None:
            self._token_count_cache.move_to_end(cache_key)
            return cached
        count_kwargs: dict[str, Any] = {
            "model": self.model,
            "input": payload["input"],
            "parallel_tool_calls": payload["parallel_tool_calls"],
            "reasoning": payload["reasoning"],
            "timeout": self._timeout_seconds,
        }
        if "instructions" in payload:
            count_kwargs["instructions"] = payload["instructions"]
        if "tools" in payload:
            count_kwargs["tools"] = payload["tools"]
        if "text" in payload:
            count_kwargs["text"] = payload["text"]
        result = await self._client.responses.input_tokens.count(**count_kwargs)
        tokens = getattr(result, "input_tokens", None)
        if type(tokens) is not int or tokens < 0:
            raise ValueError("OpenAI input token count missing")
        self._token_count_cache[cache_key] = tokens
        if len(self._token_count_cache) > _TOKEN_COUNT_CACHE_MAX:
            self._token_count_cache.popitem(last=False)
        return tokens

    async def generate_content_async(self, llm_request: LlmRequest, stream: bool = False):
        if stream:
            raise ValueError("streaming is not enabled for atomic WeChat turns")
        payload = self._build_payload(llm_request)
        config = llm_request.config
        temperature = getattr(config, "temperature", None)
        max_output_tokens = getattr(config, "max_output_tokens", None)
        create_kwargs: dict[str, Any] = {
            **payload,
            "temperature": 1.0 if temperature is None else temperature,
            "store": False,
            "timeout": self._timeout_seconds,
        }
        if max_output_tokens is not None:
            create_kwargs["max_output_tokens"] = max_output_tokens
        response = await self._client.responses.create(**create_kwargs)

        parts: list[types.Part] = []
        blocked = False
        for item in getattr(response, "output", None) or ():
            item_type = getattr(item, "type", None)
            if item_type == "message":
                for block in getattr(item, "content", None) or ():
                    block_type = getattr(block, "type", None)
                    if block_type == "output_text":
                        text = getattr(block, "text", None)
                        if isinstance(text, str) and text:
                            parts.append(types.Part.from_text(text=text))
                    elif block_type == "refusal":
                        blocked = True
                        refusal = getattr(block, "refusal", None)
                        if isinstance(refusal, str) and refusal:
                            parts.append(types.Part.from_text(text=refusal))
            elif item_type == "function_call":
                raw_args = getattr(item, "arguments", None) or "{}"
                try:
                    args = json.loads(raw_args) if isinstance(raw_args, str) else dict(raw_args)
                    if not isinstance(args, dict):
                        args = {"invalid_arguments": True}
                except (ValueError, TypeError):
                    args = {"invalid_arguments": True}
                call_id = getattr(item, "call_id", None) or getattr(item, "id", None)
                parts.append(
                    types.Part(
                        function_call=types.FunctionCall(
                            id=call_id,
                            name=item.name,
                            args=args,
                        )
                    )
                )

        usage = getattr(response, "usage", None)
        incoming = getattr(usage, "input_tokens", None)
        outgoing = getattr(usage, "output_tokens", None)
        usage_metadata = None
        custom_metadata: dict[str, Any] | None = None
        if type(incoming) is int and type(outgoing) is int and incoming >= 0 and outgoing >= 0:
            details = getattr(usage, "input_tokens_details", None)
            cached = int(getattr(details, "cached_tokens", 0) or 0)
            written = int(getattr(details, "cache_write_tokens", 0) or 0)
            usage_metadata = types.GenerateContentResponseUsageMetadata(
                prompt_token_count=incoming,
                candidates_token_count=outgoing,
                total_token_count=incoming + outgoing,
                cached_content_token_count=cached,
            )
            custom_metadata = {"openai_cache_write_tokens": written}

        yield LlmResponse(
            model_version=getattr(response, "model", None) or self.model,
            content=types.Content(role="model", parts=parts),
            error_code="SAFETY" if blocked else None,
            usage_metadata=usage_metadata,
            custom_metadata=custom_metadata,
        )
