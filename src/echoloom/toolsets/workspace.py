"""Sandboxed workspace filesystem tools with owner-only write/edit gating."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..file_reader import read_paginated_file_lines
from ..runtime_contract import AgentTool as ToolUnion
from ..tool_timing import agent_function_tool
from ..turn_scope import current_turn_owner_authorized

MAX_WRITE_BYTES = 1_000_000
_MAX_REVISIONS_PER_FILE = 20


def _is_link_or_junction(path: Path) -> bool:
    if path.is_symlink():
        return True
    checker = getattr(path, "is_junction", None)
    return bool(callable(checker) and checker())


class WorkspaceToolset:
    """Bounded read/write/edit toolset under data/workspace (plus read-only received-files)."""

    TOOL_NAMES: tuple[str, ...] = (
        "list_workspace_files",
        "read_workspace_file",
        "write_workspace_file",
        "edit_workspace_file",
    )

    def __init__(
        self,
        root: Path,
        *,
        command_owner_ids: frozenset[str] | set[str] = frozenset(),
    ) -> None:
        self.root = Path(root).resolve()
        self.workspace_root = (self.root / "data" / "workspace").resolve()
        self.received_files_root = (self.root / "data" / "received-files").resolve()
        self.command_owner_ids = frozenset(command_owner_ids)
        self._lock = threading.RLock()

    @staticmethod
    def _validate_rel(path: str, *, allow_empty: bool = False) -> Path:
        if not isinstance(path, str) or "\0" in path or "\\" in path:
            raise ValueError("请使用相对路径和正斜杠（禁止反斜杠或空字节）。")
        cleaned = path.strip()
        if not cleaned or cleaned == ".":
            if allow_empty:
                return Path("")
            raise ValueError("路径不能为空。")
        rel = Path(cleaned)
        if rel.is_absolute():
            raise ValueError("禁止使用绝对路径。")
        for part in rel.parts:
            if part in ("", ".", "..") or part.startswith("."):
                raise ValueError("禁止目录穿越或访问隐藏内部目录。")
        return rel

    def _resolve_under(self, base_root: Path, rel: Path, *, check_links: bool = True) -> Path:
        base = base_root.resolve()
        if base.exists() and _is_link_or_junction(base):
            raise ValueError("根目录不允许是符号链接或联接。")
        current = base
        for part in rel.parts:
            current = current / part
            if current.exists() and check_links and _is_link_or_junction(current):
                raise ValueError("目标路径包含不受支持的链接。")
        resolved = current.resolve()
        if not resolved.is_relative_to(base):
            raise ValueError("目标路径越出工作区边界。")
        return resolved

    def _read_target(self, path: str, *, allow_empty: bool = False) -> tuple[str, Path, bool]:
        rel = self._validate_rel(path, allow_empty=allow_empty)
        parts = rel.parts
        if parts and parts[0] == "received-files":
            if len(parts) > 1 and parts[1] == "sent-files":
                raise ValueError("仅支持访问 received-files 中的聊天收件。")
            sub = Path(*parts[1:]) if len(parts) > 1 else Path("")
            return "received-files", self._resolve_under(self.received_files_root, sub), True
        return "workspace", self._resolve_under(self.workspace_root, rel), False

    @staticmethod
    def _atomic_write(target: Path, data: bytes) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as handle:
                temporary = Path(handle.name)
                handle.write(data)
            os.replace(temporary, target)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def _save_revision(self, rel: Path, original: bytes, *, reason: str) -> None:
        rev_dir = self.workspace_root / ".revisions"
        if rev_dir.exists() and _is_link_or_junction(rev_dir):
            raise ValueError("工作区快照目录无效。")
        rev_dir.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(original).hexdigest()
        slug = re.sub(r"[^a-zA-Z0-9_.-]+", "_", rel.as_posix())[:80]
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        rev_path = rev_dir / f"{slug}-{stamp}-{digest[:12]}.json"
        record = {
            "path": rel.as_posix(),
            "date": datetime.now(UTC).isoformat(),
            "reason": reason,
            "sha256": digest,
            "content": original.decode("utf-8", errors="replace"),
        }
        self._atomic_write(
            rev_path,
            json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        )
        stale = sorted(rev_dir.glob(f"{slug}-*.json"), key=lambda p: p.name, reverse=True)[_MAX_REVISIONS_PER_FILE:]
        for old in stale:
            if not _is_link_or_junction(old):
                old.unlink(missing_ok=True)

    def list_workspace_files(self, path: str = "", offset: int = 0) -> dict[str, object]:
        """List files in data/workspace (or read-only 'received-files')."""
        if type(offset) is not int or offset < 0:
            return {"status": "rejected", "reason": "offset 必须为非负整数。"}
        try:
            area, target, read_only = self._read_target(path, allow_empty=True)
            if not target.exists():
                if not path.strip():
                    return {
                        "status": "completed",
                        "area": area,
                        "path": "",
                        "read_only": read_only,
                        "items": [],
                        "next_offset": None,
                    }
                raise ValueError("指定目录不存在。")
            if not target.is_dir():
                raise ValueError("目标不是目录。")
            base = self.received_files_root if read_only else self.workspace_root
            items: list[dict[str, object]] = []
            for child in sorted(target.iterdir(), key=lambda p: p.name.casefold()):
                if child.name.startswith(".") or _is_link_or_junction(child):
                    continue
                if read_only and child.name == "sent-files":
                    continue
                resolved = child.resolve()
                if not resolved.is_relative_to(base):
                    continue
                entry: dict[str, object] = {
                    "name": child.name,
                    "type": "directory" if child.is_dir() else "file",
                }
                if child.is_file():
                    st = child.stat()
                    entry["size_bytes"] = st.st_size
                items.append(entry)
            return {
                "status": "completed",
                "area": area,
                "path": path.strip(),
                "read_only": read_only,
                "items": items[offset : offset + 100],
                "next_offset": offset + 100 if len(items) > offset + 100 else None,
            }
        except (OSError, ValueError) as exc:
            return {"status": "rejected", "reason": str(exc)}

    def read_workspace_file(
        self,
        path: str,
        start_line: int = 1,
        line_count: int = 150,
    ) -> dict[str, object]:
        """Read a file inside data/workspace or read-only data/received-files (incl. text PDFs) with line pagination."""
        if type(start_line) is not int or type(line_count) is not int:
            return {"status": "rejected", "reason": "start_line 和 line_count 必须为整数。"}
        try:
            area, target, read_only = self._read_target(path)
            if not target.is_file():
                raise ValueError("目标文件不存在。")
            parsed = read_paginated_file_lines(target, start_line=start_line, line_count=line_count)
            return {
                "status": "completed",
                "area": area,
                "path": path.strip(),
                "read_only": read_only,
                **parsed,
            }
        except (OSError, ValueError, UnicodeError) as exc:
            return {"status": "rejected", "reason": str(exc)}

    def write_workspace_file(
        self,
        path: str,
        content: str,
        overwrite: bool = False,
        expected_sha256: str | None = None,
    ) -> dict[str, object]:
        """Create or replace a UTF-8 file inside data/workspace (owner-authorized turns only)."""
        authorized, reason = current_turn_owner_authorized(self.command_owner_ids)
        if not authorized:
            return {"status": "rejected", "reason": reason}
        if not isinstance(content, str):
            return {"status": "rejected", "reason": "content 必须为字符串。"}
        encoded = content.encode("utf-8")
        if len(encoded) > MAX_WRITE_BYTES:
            return {"status": "rejected", "reason": "文件体积超出工作区写入上限（1MB）。"}
        try:
            rel = self._validate_rel(path)
            if rel.parts[0] == "received-files":
                raise ValueError("received-files 为只读目录，请写入 workspace 根目录内。")
            with self._lock:
                self.workspace_root.mkdir(parents=True, exist_ok=True)
                target = self._resolve_under(self.workspace_root, rel)
                if target.exists() and target.is_dir():
                    raise ValueError("目标路径是一个目录。")
                previous_sha: str | None = None
                if target.exists():
                    if not overwrite:
                        raise ValueError("文件已存在；请改用 edit_workspace_file 或传 overwrite=True。")
                    original = target.read_bytes()
                    previous_sha = hashlib.sha256(original).hexdigest()
                    if expected_sha256 is not None and previous_sha != expected_sha256.strip().lower():
                        raise ValueError("expected_sha256 与当前文件内容不匹配，请重新读取后再写。")
                    self._save_revision(rel, original, reason="before_write")
                elif expected_sha256:
                    raise ValueError("文件尚未创建，不应提供 expected_sha256。")
                self._atomic_write(target, encoded)
                return {
                    "status": "written",
                    "path": rel.as_posix(),
                    "previous_sha256": previous_sha,
                    "sha256": hashlib.sha256(encoded).hexdigest(),
                    "byte_count": len(encoded),
                }
        except (OSError, ValueError, UnicodeError) as exc:
            return {"status": "rejected", "reason": str(exc)}

    def edit_workspace_file(
        self,
        path: str,
        old_text: str,
        new_text: str,
        expected_sha256: str | None = None,
    ) -> dict[str, object]:
        """Apply a unique exact-match text replacement inside data/workspace (owner-only)."""
        authorized, reason = current_turn_owner_authorized(self.command_owner_ids)
        if not authorized:
            return {"status": "rejected", "reason": reason}
        if not isinstance(old_text, str) or not old_text or not isinstance(new_text, str):
            return {"status": "rejected", "reason": "old_text 必须非空，new_text 必须为字符串。"}
        try:
            rel = self._validate_rel(path)
            if rel.parts[0] == "received-files":
                raise ValueError("received-files 为只读目录，禁止直接修改。")
            with self._lock:
                target = self._resolve_under(self.workspace_root, rel)
                if not target.is_file():
                    raise ValueError("目标文件不存在。")
                original = target.read_bytes()
                if b"\x00" in original[:8192]:
                    raise ValueError("二进制文件不支持文本局部编辑。")
                previous_sha = hashlib.sha256(original).hexdigest()
                if expected_sha256 is not None and previous_sha != expected_sha256.strip().lower():
                    raise ValueError("expected_sha256 与当前文件内容不匹配，请重新读取后再编辑。")
                text = original.decode("utf-8")
                occurrences = text.count(old_text)
                if occurrences != 1:
                    raise ValueError(
                        f"old_text 必须在文件中恰好出现一次（当前匹配 {occurrences} 次）。"
                    )
                updated_bytes = text.replace(old_text, new_text, 1).encode("utf-8")
                if len(updated_bytes) > MAX_WRITE_BYTES:
                    raise ValueError("编辑后的文件体积超出上限（1MB）。")
                self._save_revision(rel, original, reason="before_edit")
                self._atomic_write(target, updated_bytes)
                return {
                    "status": "edited",
                    "path": rel.as_posix(),
                    "previous_sha256": previous_sha,
                    "sha256": hashlib.sha256(updated_bytes).hexdigest(),
                    "byte_count": len(updated_bytes),
                }
        except (OSError, ValueError, UnicodeError) as exc:
            return {"status": "rejected", "reason": str(exc)}

    def agent_tools(self) -> list[ToolUnion]:
        return [
            agent_function_tool(fn)
            for fn in (
                self.list_workspace_files,
                self.read_workspace_file,
                self.write_workspace_file,
                self.edit_workspace_file,
            )
        ]

    def adk_tools(self) -> list[Any]:
        from ..frameworks.adk import to_adk
        return [to_adk(tool) for tool in self.agent_tools()]
