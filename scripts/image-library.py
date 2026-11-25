"""Human CLI for importing and arranging the local Agent image library."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from wechat_agent.memory_store import MemoryStore  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="管理 Agent 的本地图片/表情图库")
    parser.add_argument("--account-id", help="当前微信账号 wxid；单账号数据库可省略")
    sub = parser.add_subparsers(dest="command", required=True)
    listing = sub.add_parser("list", help="查看图库")
    listing.add_argument("--conversation-id", help="按聊天 ID 筛选；省略时看完整图库")
    listing.add_argument("--offset", type=int, default=0)
    importer = sub.add_parser("import", help="导入文件或目录中的图片")
    importer.add_argument("path", type=Path)
    importer.add_argument("--hide-from-prompt", action="store_true")
    importer.add_argument("--order", type=int, default=100)
    update = sub.add_parser("set", help="设置图片描述、快速发送和展示顺序")
    update.add_argument("image_id")
    update.add_argument("--description")
    update.add_argument("--quick-send", choices=("yes", "no"))
    update.add_argument("--order", type=int)
    args = parser.parse_args()

    store = MemoryStore(ROOT / "data" / "agent-memory.sqlite3")
    try:
        account_id = args.account_id
        if not account_id:
            accounts = [row[0] for row in store.db.execute(
                "SELECT DISTINCT account_id FROM source_messages ORDER BY account_id"
            )]
            if len(accounts) != 1:
                parser.error("数据库不是单账号状态，请加 --account-id 指定账号 wxid。")
            account_id = str(accounts[0])
        if args.command == "list":
            images = store.list_images(
                account_id=account_id,
                conversation_id=args.conversation_id,
                limit=50, offset=args.offset,
            )
            print(json.dumps([
                {"image_id": item["image_id"],
                 "description": item["description"], "quick_send": bool(item["quick_send"]),
                 "display_order": item["display_order"],
                 "conversation_id": item["conversation_id"]}
                for item in images
            ], ensure_ascii=False, indent=2))
        elif args.command == "set":
            print(json.dumps(store.manage_image_library(
                account_id=account_id, image_id=args.image_id, actor="manual",
                description=args.description,
                quick_send=(None if args.quick_send is None else args.quick_send == "yes"),
                display_order=args.order,
            ), ensure_ascii=False, indent=2))
        else:
            source = args.path.resolve(strict=True)
            files = sorted(
                (p for p in (source.rglob("*") if source.is_dir() else [source])
                 if p.is_file() and p.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".webp"}),
                key=lambda p: p.name.casefold(),
            )
            results = []
            for index, path in enumerate(files):
                try:
                    results.append(store.import_library_image(
                        account_id=account_id, source_path=path,
                        description=path.stem[:300],
                        display_order=args.order + index,
                        quick_send=not args.hide_from_prompt,
                    ))
                except (OSError, ValueError) as exc:
                    results.append({"file": path.name, "status": "rejected", "reason": str(exc)})
            print(json.dumps({"imported_or_updated": len(files), "results": results},
                             ensure_ascii=False, indent=2))
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
