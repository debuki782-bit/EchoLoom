#!/usr/bin/env python
"""Inspect WeChat agent session traces.

Lists traced chats, or renders one chat's transcript: the exact model input
per turn (including the dynamic runtime tail), model replies, and every tool
call with its result. Read-only.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from wechat_agent.trace_view import (
    DEFAULT_TRACE_DIR,
    load_records,
    render,
    summarize,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="查看会话 trace：模型看到了什么、说了什么、执行了什么工具。",
    )
    parser.add_argument(
        "--trace-dir",
        type=Path,
        default=DEFAULT_TRACE_DIR,
        help=f"trace 目录（默认 {DEFAULT_TRACE_DIR}）",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="列出有 trace 的聊天及其轮数/工具调用数",
    )
    parser.add_argument(
        "--conversation",
        default="",
        help="聊天 ID（支持前缀匹配，如 wxid_abc 或 idle-event:）",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="不截断长文本，完整输出",
    )
    parser.add_argument(
        "--jsonl",
        action="store_true",
        help="按原始 JSONL 输出（配合 --conversation）",
    )
    args = parser.parse_args()

    if not args.trace_dir.exists():
        print(f"trace 目录不存在：{args.trace_dir}", file=sys.stderr)
        return 1

    if args.list or not args.conversation:
        rows = summarize(args.trace_dir)
        if not rows:
            print("没有可用 trace。")
            return 0
        print(f"{'聊天/会话':<40} {'代':>3} {'轮':>4} {'工具':>4}  时间范围")
        for row in rows:
            print(
                f"{str(row['conversation_id']):<40} {row['sessions']:>3} "
                f"{row['turns']:>4} {row['tool_calls']:>4}  "
                f"{row['first']} ~ {row['last']}"
            )
        return 0

    records = load_records(args.trace_dir, conversation_prefix=args.conversation)
    if not records:
        print(f"没有匹配 “{args.conversation}” 的 trace 记录。", file=sys.stderr)
        return 1

    if args.jsonl:
        for record in records:
            clean = {
                key: value for key, value in record.items() if key != "_file"
            }
            print(json.dumps(clean, ensure_ascii=False))
        return 0

    print(render(records, full=args.full))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
