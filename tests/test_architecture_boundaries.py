"""Automated architecture boundary and stale-reference guard tests."""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_PKG = ROOT / "src" / "echoloom"

# Explicit monotonic dependency layering: a module in layer N may only import
# same-package modules from layers <= N at module import time.
MODULE_LAYERS: dict[str, int] = {
    # Layer 0: Shared primitives, contracts, config, pure domain policies.
    "__init__": 0,
    "agent_profiles": 0,
    "bilibili": 0,
    "contacts": 0,
    "credentials": 0,
    "db_async": 0,
    "file_reader": 0,
    "freshness_policy": 0,
    "gateway_ingress": 0,
    "local_config": 0,
    "media_artifacts": 0,
    "mock": 0,
    "model_catalog": 0,
    "model_overrides": 0,
    "models": 0,
    "ports": 0,
    "prompt_catalog": 0,
    "runtime_contract": 0,
    "session_history": 0,
    "session_recycle_policy": 0,
    "temporal_context": 0,
    "tool_batch": 0,
    "tool_policy": 0,
    "tool_timing": 0,
    "trace_view": 0,
    "wxgf": 0,
    # Layer 1: Channel connectors, Hook adapters, media ingestion & context models.
    "adapters.__init__": 1,
    "adapters.hook": 1,
    "adapters.hook_contacts": 1,
    "adapters.hook_event_extension": 1,
    "adapters.hook_events": 1,
    "adapters.hook_media": 1,
    "adapters.hook_native_images": 1,
    "adapters.hook_recovery": 1,
    "adapters.hook_text": 1,
    "adapters.recording": 1,
    "channel_gateway": 1,
    "connectors.__init__": 1,
    "connectors.base": 1,
    "connectors.discord": 1,
    "connectors.feishu": 1,
    "connectors.recording": 1,
    "connectors.registry": 1,
    "connectors.telegram": 1,
    "connectors.webhook": 1,
    "connectors.wechat_hook": 1,
    "context": 1,
    "hook_contract": 1,
    "hook_outbound": 1,
    "hook_status_recovery": 1,
    "media": 1,
    "native_image_download": 1,
    "rich_message": 1,
    "wechat_gateway": 1,
    # Layer 2: SQLite persistence, usage ledger, repositories & schedulers.
    "conversation_scheduler": 2,
    "episode_memory": 2,
    "event_book": 2,
    "memory_store": 2,
    "outbound_store": 2,
    "repos.__init__": 2,
    "repos.enrichment": 2,
    "repos.knowledge_repo": 2,
    "repos.media_repo": 2,
    "repos.message_repo": 2,
    "repos.schema": 2,
    "repos.short_ref_repo": 2,
    "runtime_scope": 2,
    "storage": 2,
    "time_scheduler": 2,
    "usage": 2,
    # Layer 3: Ingestion, read-only context assembly & cross-session lookup.
    "context_assembler": 3,
    "context_builder": 3,
    "cross_session": 3,
    "cross_session_sql": 3,
    "message_ingestor": 3,
    # Layer 4: Turn scope, ingress, outbound, dispatcher, triggers & Pilot facade.
    "dispatcher": 4,
    "dispatcher_support": 4,
    "ingress": 4,
    "outbound": 4,
    "pilot": 4,
    "triggers": 4,
    "turn_scope": 4,
    # Layer 5: Model providers, ADK frameworks, toolsets, runtime, dashboard & bootstrap.
    "adk_budget": 5,
    "adk_runner": 5,
    "agent_tools": 5,
    "bootstrap": 5,
    "channel_tools": 5,
    "cli": 5,
    "cross_chat_dedupe": 5,
    "cross_session_tools": 5,
    "dashboard_memory": 5,
    "frameworks.__init__": 5,
    "frameworks.adk": 5,
    "frameworks.adk_budget": 5,
    "frameworks.adk_openai": 5,
    "frameworks.adk_runner": 5,
    "frameworks.google_web": 5,
    "idle_tools": 5,
    "jev_strategy": 5,
    "project_tools": 5,
    "providers": 5,
    "runtime_context_hook": 5,
    "runtime_providers": 5,
    "runtime_route": 5,
    "runtime_runners": 5,
    "session_rollover": 5,
    "session_runtime": 5,
    "toolsets.__init__": 5,
    "toolsets.media": 5,
    "toolsets.memory": 5,
    "toolsets.reply": 5,
    "toolsets.workspace": 5,
    "trace_dashboard": 5,
    "turn_hooks": 5,
    "web_tools": 5,
    "wechat_tools": 5,
}


def _module_name(path: Path) -> str:
    rel = path.relative_to(SRC_PKG).with_suffix("")
    return ".".join(rel.parts)


def _is_type_checking_if(node: ast.If) -> bool:
    test = node.test
    if isinstance(test, ast.Name) and test.id == "TYPE_CHECKING":
        return True
    return bool(isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING")


def _resolve_relative(current_module: str, level: int, target: str | None) -> str:
    parts = current_module.split(".")[:-level]
    if target:
        parts.extend(target.split("."))
    elif not parts:
        return "__init__"
    candidate = ".".join(parts)
    if candidate in MODULE_LAYERS:
        return candidate
    if f"{candidate}.__init__" in MODULE_LAYERS:
        return f"{candidate}.__init__"
    return candidate


def _collect_runtime_imports(path: Path, current_module: str) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: list[str] = []

    def walk(stmts: list[ast.stmt]) -> None:
        for stmt in stmts:
            if isinstance(stmt, ast.If):
                if not _is_type_checking_if(stmt):
                    walk(stmt.body)
                walk(stmt.orelse)
                continue
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                # Only module-level statements create import-time edges; classes still execute bodies.
                if isinstance(stmt, ast.ClassDef):
                    walk(stmt.body)
                continue
            if isinstance(stmt, ast.Try):
                walk(stmt.body)
                for handler in stmt.handlers:
                    walk(handler.body)
                walk(stmt.orelse)
                walk(stmt.finalbody)
                continue
            if isinstance(stmt, ast.With):
                walk(stmt.body)
                continue
            if isinstance(stmt, ast.Import):
                for alias in stmt.names:
                    if alias.name == "echoloom":
                        found.append("__init__")
                    elif alias.name.startswith("echoloom."):
                        found.append(alias.name.removeprefix("echoloom."))
            elif isinstance(stmt, ast.ImportFrom):
                if stmt.level > 0:
                    resolved = _resolve_relative(current_module, stmt.level, stmt.module)
                    found.append(resolved)
                elif stmt.module == "echoloom":
                    for alias in stmt.names:
                        candidate = alias.name
                        if candidate in MODULE_LAYERS:
                            found.append(candidate)
                        elif f"{candidate}.__init__" in MODULE_LAYERS:
                            found.append(f"{candidate}.__init__")
                        else:
                            found.append("__init__")
                elif stmt.module and stmt.module.startswith("echoloom."):
                    found.append(stmt.module.removeprefix("echoloom."))

    walk(tree.body)
    return found


def test_every_package_module_has_explicit_layer_assignment() -> None:
    discovered = {_module_name(path) for path in SRC_PKG.rglob("*.py")}
    assert discovered == set(MODULE_LAYERS)


def test_module_imports_strictly_obey_downward_layer_order() -> None:
    violations: list[str] = []
    for path in sorted(SRC_PKG.rglob("*.py")):
        src_mod = _module_name(path)
        src_layer = MODULE_LAYERS[src_mod]
        for dst_mod in _collect_runtime_imports(path, src_mod):
            if dst_mod not in MODULE_LAYERS:
                continue
            dst_layer = MODULE_LAYERS[dst_mod]
            if dst_layer > src_layer:
                violations.append(
                    f"{src_mod} (L{src_layer}) imports {dst_mod} (L{dst_layer})"
                )
    assert violations == []


def test_pure_policy_modules_stay_zero_io_and_zero_sdk() -> None:
    forbidden_roots = {
        "asyncio",
        "datetime",
        "fastapi",
        "google",
        "httpx",
        "openai",
        "os",
        "pathlib",
        "sqlite3",
        "time",
        "uvicorn",
    }
    for name in ("freshness_policy.py", "session_recycle_policy.py"):
        tree = ast.parse((SRC_PKG / name).read_text(encoding="utf-8"), filename=name)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name.split(".")[0] not in forbidden_roots, (name, alias.name)
            elif isinstance(node, ast.ImportFrom) and node.module:
                assert node.module.split(".")[0] not in forbidden_roots, (name, node.module)


def test_removed_openai_agents_sdk_has_no_stale_references() -> None:
    lock_text = (ROOT / "requirements.lock").read_text(encoding="utf-8")
    assert "openai-agents==" not in lock_text
    arch_doc = (ROOT / "docs" / "architecture" / "current-architecture.md").read_text(
        encoding="utf-8"
    )
    assert "OpenAI Agents SDK" not in arch_doc
    assert "openai-sessions" not in arch_doc
    bootstrap_doc = ast.get_docstring(
        ast.parse((SRC_PKG / "bootstrap.py").read_text(encoding="utf-8"))
    )
    assert bootstrap_doc is not None and "``agents``" not in bootstrap_doc
