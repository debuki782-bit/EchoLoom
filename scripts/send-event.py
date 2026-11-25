"""Send normalized events to EchoLoom's multi-source HTTP Ingress without WeChat."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys

import httpx


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080", help="EchoLoom Ingress base URL")
    parser.add_argument("--source", default="webhook", help="Registered source_id in sources.toml")
    parser.add_argument("--account", default="", help="Target bot account_id (optional)")
    parser.add_argument("--conversation", default="room:cli-demo", help="Conversation/room ID")
    parser.add_argument("--sender", default="user:cli", help="Sender ID")
    parser.add_argument("--sender-name", default="CLI User", help="Optional sender display name")
    parser.add_argument("--text", required=True, help="Message body text")
    parser.add_argument("--group", action="store_true", help="Mark as group conversation")
    parser.add_argument("--token", default=os.environ.get("ECHOLOOM_WEBHOOK_SECRET", ""), help="Bearer token")
    parser.add_argument("--hmac-secret", default="", help="Optional HMAC-SHA256 signing secret")
    args = parser.parse_args(argv)

    event: dict[str, object] = {
        "conversation_id": args.conversation,
        "sender_id": args.sender,
        "sender_name": args.sender_name,
        "text": args.text,
        "is_group": bool(args.group or args.conversation.startswith("room:")),
    }
    if args.account:
        event["account_id"] = args.account

    encoded = json.dumps({"events": [event]}, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if args.token:
        headers["Authorization"] = f"Bearer {args.token}"
    if args.hmac_secret:
        sig = hmac.new(args.hmac_secret.encode("utf-8"), encoded, hashlib.sha256).hexdigest()
        headers["X-Signature-256"] = f"sha256={sig}"

    endpoint = f"{args.url.rstrip('/')}/sources/{args.source}/events"
    with httpx.Client(timeout=10.0, trust_env=False) as client:
        response = client.post(endpoint, content=encoded, headers=headers)
    print(f"HTTP {response.status_code}: {response.text}")
    return 0 if response.is_success else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
