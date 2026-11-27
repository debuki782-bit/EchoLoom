"""Profile chat runner cache, idle runners, and owner special command handler."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

from .agent_profiles import AgentProfile, AgentProfiles
from .connectors.base import canonical_content_kind
from .model_catalog import load_model_catalog
from .runtime_route import ProviderBundle, RunnerBundle, ServiceBundle

logger = logging.getLogger("echoloom.startup")


def build_runners(
    root: Path,
    account: str,
    profile_config: AgentProfiles,
    providers: ProviderBundle,
    services: ServiceBundle,
    *,
    runtime_cls_resolver: Callable[[], Any],
    supported_models: tuple[str, ...],
) -> RunnerBundle:
    """Build main chat profile runner cache, idle runners, and owner command handler."""
    from .frameworks.adk import AdkFramework
    from .idle_tools import IDLE_SUMMARY_MAX_MODEL_CALLS, IDLE_WORK_MAX_MODEL_CALLS
    from .model_overrides import MODEL_LABELS, model_list_text, parse_model_command
    from .prompt_catalog import refresh_prompts

    workspace = Path(root)
    catalog = load_model_catalog(workspace=workspace)
    runtime_cls = runtime_cls_resolver()
    default_profile = profile_config.profiles[profile_config.default_profile]
    runner_cache: dict[tuple[str, str], Any] = {}

    def count_effective_messages(
        account_id: str,
        conversation_id: str,
        since: datetime,
    ) -> int:
        return services.memory_store.count_effective_messages(
            account_id=account_id,
            conversation_id=conversation_id,
            since=since,
        )

    def runner_for(profile: AgentProfile, model_id: str | None = None):
        selected = replace(profile, model_id=model_id) if model_id else profile
        cache_key = (profile.profile_id, selected.model_id)
        cached = runner_cache.get(cache_key)
        if cached is not None:
            return cached
        profile_model = providers.make_adk_model(selected)
        profile_summary_model = providers.make_adk_model(
            replace(selected, model_id=profile_config.memory_model_id),
        )
        model_spec = catalog.get(selected.model_id)
        effective_cache_tier = selected.cache_tier or (
            model_spec.cache_tier if model_spec is not None else None
        )
        effective_cache_idle = (
            selected.cache_recycle_idle_seconds
            if selected.cache_recycle_idle_seconds is not None
            else (model_spec.cache_recycle_idle_seconds if model_spec is not None else None)
        )
        effective_cache_min_tokens = (
            selected.cache_recycle_min_tokens
            if selected.cache_recycle_min_tokens is not None
            else (model_spec.cache_recycle_min_tokens if model_spec is not None else None)
        )
        extra_tools = services.profile_tools(profile, profile_model)
        runner = runtime_cls(
            workspace / "data" / "live-sessions.sqlite3",
            model=profile_model,
            framework=AdkFramework(project_compaction=True),
            summary_model=profile_summary_model,
            summary_framework=AdkFramework(project_compaction=True),
            extra_tools=extra_tools,
            idle_session_hours=profile.idle_session_hours,
            max_history_records=profile.max_history_records,
            max_history_bytes=profile.max_history_bytes,
            token_context_limits=profile.token_context_limits,
            event_book_context=services.event_book.render,
            effective_message_count=count_effective_messages,
            last_human_message_age=lambda account_id, conversation_id: (
                services.memory_store.minutes_since_last_human_message(
                    account_id=account_id,
                    conversation_id=conversation_id,
                )
            ),
            jev_label_sink=lambda labels: services.memory_store.save_jev_labels(
                account_id=account,
                labels=labels,
            ),
            episode_memory=services.episode_memory,
            profile_id=profile.profile_id,
            persona_prompt=profile.persona_prompt,
            main_prompt=profile.main_prompt,
            summary_prompt=profile.summary_prompt,
            summary_system_prompt=profile.summary_system_prompt,
            profile_daily_budget_microusd=profile.daily_budget_microusd,
            profile_text_budget_microusd=profile.text_budget_microusd,
            profile_image_read_budget_microusd=profile.image_read_budget_microusd,
            profile_image_generation_budget_microusd=profile.image_generation_budget_microusd,
            profile_for_conversation=(
                lambda account_id, conversation_id: profile_config.for_conversation(
                    account_id,
                    conversation_id,
                ).profile_id
            ),
            cache_tier=effective_cache_tier,
            cache_recycle_idle_seconds=effective_cache_idle,
            cache_recycle_min_tokens=effective_cache_min_tokens,
            clock=providers.clock,
        )
        runner_cache[cache_key] = runner
        logger.info(
            "event=agent_profile_loaded profile=%s model=%s tools=%s",
            profile.profile_id,
            selected.model_id,
            ",".join(profile.toolsets),
        )
        return runner

    def runner_for_conversation(conversation_id: str):
        profile = profile_config.for_conversation(account, conversation_id)
        override = services.model_overrides.get(account, conversation_id)
        if override and override not in supported_models:
            logger.warning("event=legacy_model_override_ignored runtime=google_adk")
            override = None
        return runner_for(profile, override or profile.model_id)

    runner_for(default_profile)

    logger.info(
        "event=idle_runtime_configured work_model_calls=%s summary_model_calls=%s "
        "summary_ends_turn=true",
        IDLE_WORK_MAX_MODEL_CALLS,
        IDLE_SUMMARY_MAX_MODEL_CALLS,
    )
    idle_runners: dict[str, Any] = {}
    for idle_model_id in (profile_config.memory_model_id,):
        idle_model = providers.make_adk_model(
            replace(default_profile, model_id=idle_model_id)
        )
        idle_runners[idle_model_id] = runtime_cls(
            workspace / "data" / "idle-sessions.sqlite3",
            model=idle_model,
            framework=AdkFramework(project_compaction=True),
            summary_model=idle_model,
            summary_framework=AdkFramework(project_compaction=True),
            profile_id=default_profile.profile_id,
            persona_prompt=default_profile.persona_prompt,
            main_prompt="idle_agent",
            summary_prompt=default_profile.summary_prompt,
            summary_system_prompt=default_profile.summary_system_prompt,
            idle_session_hours=default_profile.idle_session_hours,
            max_history_records=default_profile.max_history_records,
            max_history_bytes=default_profile.max_history_bytes,
            token_context_limits=default_profile.token_context_limits,
            profile_daily_budget_microusd=default_profile.daily_budget_microusd,
            profile_text_budget_microusd=default_profile.text_budget_microusd,
            profile_image_read_budget_microusd=default_profile.image_read_budget_microusd,
            profile_image_generation_budget_microusd=default_profile.image_generation_budget_microusd,
            extra_tools=(
                services.profile_tools(default_profile, idle_model)
                + services.idle_tools.agent_tools()
            ),
            event_book_context=services.event_book.render,
            effective_message_count=count_effective_messages,
        )

    async def reply_with_profile(window) -> str:
        return await runner_for_conversation(window.conversation_id).reply(window)

    async def handle_special_command(item) -> str | None:
        if item.sender_id not in profile_config.command_owner_ids:
            return None
        if canonical_content_kind(item) != "text":
            return None
        text = item.text.strip()
        if text in {"刷新提示词", "重新加载提示词"}:
            ok, result = refresh_prompts()
            if ok:
                runner_cache.clear()
                return "提示词文件已重新读入内存，之后的模型请求会使用新版本。"
            return f"提示词刷新失败（{result}）；旧版本仍在使用，没有切换到不完整内容。"
        command = parse_model_command(text)
        if command is None:
            return None
        kind, model_id = command
        if kind == "list":
            return model_list_text(supported_models)
        if kind == "default":
            services.model_overrides.clear(account, item.conversation_id)
            profile = profile_config.for_conversation(account, item.conversation_id)
            return f"已恢复这个聊天的档案默认模型：{MODEL_LABELS[profile.model_id]}。"
        if model_id not in supported_models:
            return "当前运行时使用 Google ADK。" + model_list_text(supported_models)
        temporary_seconds = 3600 if model_id == "gemini-4-argon" else None
        services.model_overrides.set(
            account,
            item.conversation_id,
            model_id,
            temporary_seconds=temporary_seconds,
        )
        suffix = (
            "，一小时后自动恢复档案默认模型"
            if temporary_seconds
            else "，持续使用到你再次切换"
        )
        return f"这个聊天已切换为{MODEL_LABELS[model_id]}{suffix}。"

    return RunnerBundle(
        runner_cache=runner_cache,
        runner_for=runner_for,
        runner_for_conversation=runner_for_conversation,
        idle_runners=idle_runners,
        reply_with_profile=reply_with_profile,
        handle_special_command=handle_special_command,
    )
