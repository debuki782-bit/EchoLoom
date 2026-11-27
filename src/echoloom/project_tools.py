"""Bounded project inspection and reversible edits to the agent's own prompts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import threading
from datetime import UTC, datetime
from pathlib import Path

from .runtime_contract import AgentTool as ToolUnion
from .tool_timing import agent_function_tool

_READ_ROOTS = frozenset({"docs", "prompts", "prompts.local", "src", "scripts"})
_VAULT_PROMPT_PREFIX = (".local", "prompts")
_ROOT_FILES = frozenset({
    "README.md",
    "pyproject.toml",
    "config/models.toml",
    "config/agent_profiles.toml",
    "config/runtime.example.toml",
    "config/sources.example.toml",
})
_READ_SUFFIXES = frozenset({".md", ".txt", ".json", ".yaml", ".yml", ".toml", ".py"})
_EDITABLE = frozenset({
    "persona.md", "main_agent.md", "idle_agent.md", "idle_fallback.md", "idle_finalize.md",
    "session_summary.md", "session_summary_system.md", "cross_session_task.md",
    "jev_strategy.md",
})
_REVISION_ID = re.compile(r"^[a-z0-9_-]+-[0-9]{8}T[0-9]{12}Z-[0-9a-f]{12}$")


class ProjectTools:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self._lock = threading.RLock()

    @staticmethod
    def _replace_file(target: Path, content: bytes) -> None:
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as handle:
                temporary = Path(handle.name)
                handle.write(content)
            os.replace(temporary, target)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def _read_path(self, path: str) -> Path:
        if not isinstance(path, str) or not path or "\0" in path or "\\" in path:
            raise ValueError("请使用项目内相对路径和正斜杠。")
        relative = Path(path)
        is_vault_prompt = (
            len(relative.parts) >= 3
            and relative.parts[:2] == _VAULT_PROMPT_PREFIX
            and not any(part in ("..", ".") or part.startswith(".") for part in relative.parts[2:])
        )
        if relative.is_absolute() or (
            not is_vault_prompt
            and any(part in ("..", ".") or part.startswith(".") for part in relative.parts)
        ):
            raise ValueError("路径不在允许范围。")
        if is_vault_prompt:
            allowed_base = (self.root / ".local" / "prompts").resolve()
        elif relative.parts and relative.parts[0] in _READ_ROOTS:
            allowed_base = (self.root / relative.parts[0]).resolve()
        elif path in _ROOT_FILES:
            allowed_base = self.root
        else:
            raise ValueError("路径不在允许范围。")
        target = (self.root / relative).resolve()
        if not target.is_relative_to(self.root) or not target.is_relative_to(allowed_base):
            raise ValueError("路径不在允许范围。")
        # Never follow a symlink or junction, including ones into another allowed root.
        current = self.root
        for part in relative.parts:
            current = current / part
            if current.is_symlink() or (hasattr(current, "is_junction") and current.is_junction()):
                raise ValueError("不支持链接路径。")
        if target.is_file() and target.suffix.lower() not in _READ_SUFFIXES:
            raise ValueError("不支持该文件类型。")
        return target

    def list_project_resources(self, path: str = "docs", offset: int = 0) -> dict:
        try:
            directory = self._read_path(path)
            if not directory.is_dir() or not 0 <= offset <= 10000:
                raise ValueError("目录或分页无效。")
            items = []
            for child in sorted(directory.iterdir(), key=lambda item: item.name.casefold()):
                if child.name.startswith(".") or child.is_symlink() or (
                    hasattr(child, "is_junction") and child.is_junction()
                ):
                    continue
                if child.is_dir() or (child.is_file() and child.suffix.lower() in _READ_SUFFIXES):
                    items.append({"name": child.name, "type": "directory" if child.is_dir() else "file"})
            return {"status": "completed", "path": path, "items": items[offset:offset + 100],
                    "next_offset": offset + 100 if len(items) > offset + 100 else None}
        except (OSError, ValueError) as exc:
            return {"status": "rejected", "reason": str(exc)}

    def read_project_resource(self, path: str, start_line: int = 1, line_count: int = 100) -> dict:
        try:
            target = self._read_path(path)
            if not target.is_file() or not 1 <= start_line or not 1 <= line_count <= 200:
                raise ValueError("文件或行号无效。")
            if target.stat().st_size > 1_000_000:
                raise ValueError("文件太大，不能读取。")
            raw = target.read_bytes()
            lines = raw.decode("utf-8-sig").splitlines()
            chosen = lines[start_line - 1:start_line - 1 + line_count]
            text = "\n".join(chosen)
            truncated = len(text.encode("utf-8")) > 24_000
            if truncated:
                text = text.encode("utf-8")[:24_000].decode("utf-8", "ignore")
            return {"status": "completed", "path": path, "sha256": hashlib.sha256(raw).hexdigest(),
                    "start_line": start_line, "total_lines": len(lines), "content": text,
                    "truncated": truncated, "more_lines": start_line - 1 + len(chosen) < len(lines)}
        except (OSError, ValueError, UnicodeError) as exc:
            return {"status": "rejected", "reason": str(exc)}

    def _prompt_resource_path(self, filename: str) -> str:
        for prefix in (".local/prompts/", "prompts.local/"):
            local_rel = prefix + filename
            try:
                local = self._read_path(local_rel)
                if local.is_file():
                    return local_rel
            except ValueError:
                pass
        return "prompts/" + filename

    def update_own_prompt(self, filename: str, expected_sha256: str,
                          old_text: str, new_text: str) -> dict:
        """Replace one exact passage after checking its file hash."""
        if filename not in _EDITABLE:
            return {"status": "rejected", "reason": "只能修改指定的人物与 Agent 提示词。"}
        if (not isinstance(old_text, str) or not old_text or not isinstance(new_text, str)
                or len(old_text) > 12000 or len(new_text) > 12000):
            return {"status": "rejected", "reason": "替换内容为空或过长。"}
        try:
            with self._lock:
                target = self._read_path(self._prompt_resource_path(filename))
                original = target.read_bytes()
                digest = hashlib.sha256(original).hexdigest()
                if digest != expected_sha256:
                    return {"status": "rejected", "reason": "文件已变动，请重新读取。"}
                text = original.decode("utf-8")
                if text.count(old_text) != 1:
                    return {"status": "rejected", "reason": "原文必须在文件中恰好出现一次。"}
                updated = text.replace(old_text, new_text, 1).encode("utf-8")
                if not updated.strip() or len(updated) > 100_000:
                    return {"status": "rejected", "reason": "修改后的文件为空或过长。"}
                self._save_revision(filename, original, reason="before_update")
                self._replace_file(target, updated)
                return {"status": "updated", "filename": filename, "previous_sha256": digest,
                        "sha256": hashlib.sha256(updated).hexdigest(),
                        "rollback_available": True}
        except (OSError, ValueError, UnicodeError) as exc:
            return {"status": "rejected", "reason": str(exc)}

    def list_prompt_revisions(self, filename: str) -> dict:
        if filename not in _EDITABLE:
            return {"status": "rejected", "reason": "只能查看指定提示词的版本。"}
        try:
            revisions = self._read_revisions(filename)
            return {"status": "completed", "filename": filename, "revisions": revisions}
        except (OSError, ValueError, UnicodeError) as exc:
            return {"status": "rejected", "reason": str(exc)}

    def rollback_own_prompt(self, filename: str, expected_sha256: str,
                            revision: str = "latest") -> dict:
        """Restore one allowlisted prompt snapshot without Git or arbitrary commands."""
        if filename not in _EDITABLE:
            return {"status": "rejected", "reason": "只能回滚指定的人物与 Agent 提示词。"}
        if revision != "latest" and not _REVISION_ID.fullmatch(revision):
            return {"status": "rejected", "reason": "版本必须是 latest 或版本列表中的 revision ID。"}
        try:
            with self._lock:
                target = self._read_path(self._prompt_resource_path(filename))
                current = target.read_bytes()
                digest = hashlib.sha256(current).hexdigest()
                if digest != expected_sha256:
                    return {"status": "rejected", "reason": "文件已变动，请重新读取。"}
                revisions = self._read_revisions(filename)
                chosen = revisions[0] if revision == "latest" and revisions else next(
                    (item for item in revisions if item["revision"] == revision), None
                )
                if chosen is None:
                    return {"status": "rejected", "reason": "没有找到该本地快照版本。"}
                snapshot = self._revision_path(filename, chosen["revision"])
                data = json.loads(snapshot.read_text(encoding="utf-8"))
                restored = data.get("content")
                if not isinstance(restored, str) or not restored.strip() or len(restored.encode("utf-8")) > 100_000:
                    return {"status": "rejected", "reason": "快照内容无效或过长。"}
                restored_bytes = restored.encode("utf-8")
                self._save_revision(filename, current, reason="before_rollback")
                self._replace_file(target, restored_bytes)
                return {"status": "updated", "filename": filename,
                        "source": f"snapshot {chosen['revision']}",
                        "previous_sha256": digest,
                        "sha256": hashlib.sha256(restored_bytes).hexdigest()}
        except (OSError, ValueError, UnicodeError, KeyError, TypeError) as exc:
            return {"status": "rejected", "reason": str(exc)}

    def _revision_directory(self) -> Path:
        directory = self.root / ".prompt-revisions"
        if directory.is_symlink() or (hasattr(directory, "is_junction") and directory.is_junction()):
            raise ValueError("提示词版本目录不允许是链接。")
        return directory

    def _revision_path(self, filename: str, revision: str) -> Path:
        if filename not in _EDITABLE or not _REVISION_ID.fullmatch(revision):
            raise ValueError("无效的提示词快照标识。")
        directory = self._revision_directory()
        path = directory / f"{revision}.json"
        if path.is_symlink() or (hasattr(path, "is_junction") and path.is_junction()):
            raise ValueError("提示词快照不允许是链接。")
        return path

    def _read_revisions(self, filename: str) -> list[dict[str, str]]:
        directory = self._revision_directory()
        if not directory.exists():
            return []
        if not directory.is_dir():
            raise ValueError("提示词版本目录无效。")
        revisions = []
        prefix = filename.removesuffix(".md") + "-"
        for path in directory.glob(prefix + "*.json"):
            if path.is_symlink() or (hasattr(path, "is_junction") and path.is_junction()):
                continue
            try:
                item = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError, UnicodeError):
                continue
            if (isinstance(item, dict) and isinstance(item.get("revision"), str)
                    and isinstance(item.get("date"), str)):
                revisions.append({"revision": item["revision"], "date": item["date"],
                                  "reason": str(item.get("reason", "snapshot")),
                                  "sha256": str(item.get("sha256", ""))})
        return sorted(revisions, key=lambda item: item["revision"], reverse=True)[:20]

    def _save_revision(self, filename: str, content: bytes, *, reason: str) -> str:
        directory = self._revision_directory()
        directory.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(content).hexdigest()
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        revision = f"{filename.removesuffix('.md')}-{stamp}-{digest[:12]}"
        record = {"revision": revision, "date": datetime.now(UTC).isoformat(),
                  "reason": reason, "sha256": digest,
                  "content": content.decode("utf-8")}
        self._replace_file(
            self._revision_path(filename, revision),
            json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        )
        stale = sorted(directory.glob(filename.removesuffix(".md") + "-*.json"),
                       key=lambda path: path.name, reverse=True)[20:]
        for path in stale:
            if not path.is_symlink() and not (hasattr(path, "is_junction") and path.is_junction()):
                path.unlink(missing_ok=True)
        return revision

    def agent_tools(self) -> list[ToolUnion]:
        return [agent_function_tool(fn) for fn in (
            self.list_project_resources, self.read_project_resource, self.update_own_prompt,
            self.list_prompt_revisions, self.rollback_own_prompt,
        )]


    def adk_tools(self):
        """Legacy adapter; new runtimes consume agent_tools()."""
        from .frameworks.adk import to_adk
        return [to_adk(tool) for tool in self.agent_tools()]
