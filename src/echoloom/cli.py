"""Cross-platform EchoLoom CLI entrypoint."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import logging
import os
import sys
from pathlib import Path

import httpx


def _send_parser(subparsers=None) -> argparse.ArgumentParser:
    description = "Send a normalized message event to EchoLoom's HTTP Ingress"
    if subparsers is not None:
        parser = subparsers.add_parser("send", help="Send a test/bridge event to HTTP Ingress")
    else:
        parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--url", default="http://127.0.0.1:8080", help="EchoLoom Ingress base URL")
    parser.add_argument("--source", default="webhook", help="Registered source_id in sources.toml")
    parser.add_argument("--account", default="", help="Target bot account_id (optional)")
    parser.add_argument("--conversation", default="room:cli-demo", help="Conversation/room ID")
    parser.add_argument("--sender", default="user:cli", help="Sender ID")
    parser.add_argument("--sender-name", default="CLI User", help="Optional sender display name")
    parser.add_argument("--text", required=True, help="Message body text")
    parser.add_argument("--group", action="store_true", help="Mark as group conversation")
    parser.add_argument(
        "--token",
        default=os.environ.get("ECHOLOOM_WEBHOOK_SECRET", ""),
        help="Bearer token",
    )
    parser.add_argument("--hmac-secret", default="", help="Optional HMAC-SHA256 signing secret")
    return parser


def _execute_send(args: argparse.Namespace) -> int:
    event: dict[str, object] = {
        "conversation_id": args.conversation,
        "sender_id": args.sender,
        "sender_name": args.sender_name,
        "text": args.text,
        "is_group": bool(args.group or str(args.conversation).startswith("room:")),
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


def send_main(argv: list[str] | None = None) -> int:
    """Standalone CLI helper for sending a normalized event to `/sources/<id>/events`."""
    parser = _send_parser()
    args = parser.parse_args(argv)
    return _execute_send(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="echoloom",
        description="EchoLoom multi-channel personal & team agent runtime CLI",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_init = subparsers.add_parser("init", help="Initialize isolated .local runtime vault")
    p_init.add_argument("--root", type=Path, default=None, help="Workspace root directory")

    p_serve = subparsers.add_parser("serve", help="Start EchoLoom ingress, worker, or combined service")
    p_serve.add_argument("--root", type=Path, default=None, help="Workspace root directory")
    p_serve.add_argument(
        "--role",
        choices=("all", "ingress", "worker"),
        default="all",
        help="Runtime role (all, ingress, or worker)",
    )
    p_serve.add_argument("--host", default=None, help="Bind host override")
    p_serve.add_argument("--port", type=int, default=None, help="Bind port override")

    p_dash = subparsers.add_parser("dashboard", help="Run read-only local trace dashboard")
    p_dash.add_argument("--host", default=None, help="Dashboard bind host")
    p_dash.add_argument("--port", type=int, default=None, help="Dashboard bind port")

    _send_parser(subparsers)

    p_vault = subparsers.add_parser("vault", help="Inspect or audit .local configuration vault")
    p_vault.add_argument(
        "action",
        choices=("status", "audit", "runtime-json", "init"),
        default="status",
        nargs="?",
    )

    p_cred = subparsers.add_parser("credentials", help="Manage local API keys / secrets")
    p_cred.add_argument("action", choices=("store", "status", "delete"))
    p_cred.add_argument("--service", choices=("model", "openai", "webhook"), default="model")
    p_cred.add_argument("--target", default="", help="Optional custom credential target name")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Unified cross-platform CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "init":
        from .local_config import ROOT, init_local_vault

        workspace = args.root.resolve() if args.root is not None else ROOT
        result = init_local_vault(workspace)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    if args.command == "serve":
        from .bootstrap import ROOT, run_pilot

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        workspace = args.root.resolve() if args.root is not None else ROOT
        run_pilot(workspace, role=args.role, host=args.host, port=args.port)
        return 0

    if args.command == "dashboard":
        from .trace_dashboard import main as dashboard_main

        dash_argv: list[str] = []
        if args.host:
            dash_argv.extend(["--host", args.host])
        if args.port is not None:
            dash_argv.extend(["--port", str(args.port)])
        dashboard_main(dash_argv)
        return 0

    if args.command == "send":
        return _execute_send(args)

    if args.command == "vault":
        from .local_config import main as vault_main

        return int(vault_main([args.action]))

    if args.command == "credentials":
        from .credentials import main as cred_main

        cred_argv = [args.action, "--service", args.service]
        if args.target:
            cred_argv.extend(["--target", args.target])
        return int(cred_main(cred_argv))

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
