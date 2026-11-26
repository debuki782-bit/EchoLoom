"""Start the configured test-group and account-private-chat Agent on Hook port 8080."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

import httpx  # noqa: F401 - compatibility target for CLI tests patching httpx.Client
import uvicorn  # noqa: F401 - compatibility target for CLI tests patching uvicorn.run

import echoloom.bootstrap as _bootstrap

ROOT = Path(__file__).resolve().parents[1]
_COMPAT_EXPORTS = frozenset({
    "ScopedAgentRuntime",
    "WeChatTools",
    "create_pilot_app",
    "genai",
    "load_key",
    "run_worker_loop",
})


def __getattr__(name: str):
    if name in _COMPAT_EXPORTS:
        return getattr(_bootstrap, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def main(argv: Sequence[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true")
    parser.add_argument(
        "--role",
        choices=("all", "ingress", "worker"),
        default="all",
        help="Process role: all (single-process), ingress (HTTP :8080 only), worker (Agent loop only).",
    )
    args = parser.parse_args(argv)
    if not args.run:
        parser.exit(message="Pass --run to start the authorized account Pilot.\n")
    overrides = {name: globals()[name] for name in _COMPAT_EXPORTS if name in globals()}
    _bootstrap.run_pilot(ROOT, role=args.role, overrides=overrides or None)


if __name__ == "__main__":
    main()
