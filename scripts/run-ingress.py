"""Standalone zero-LLM loopback HTTP Ingress (:8080) for Scheme A dual-process mode."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

import uvicorn  # noqa: F401 - compatibility target for tests patching run_ingress.uvicorn.run

from echoloom import local_config
from echoloom.bootstrap import build_ingress_app, load_live_route, run_ingress

ROOT = Path(__file__).resolve().parents[1]
load_route = load_live_route

__all__ = ["ROOT", "build_ingress_app", "load_live_route", "load_route", "main"]


def main(argv: Sequence[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    srv = local_config.load_local_runtime_config(ROOT).server
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--host", default=srv.ingress_host)
    parser.add_argument("--port", type=int, default=srv.ingress_port)
    args = parser.parse_args(argv)
    if not args.run:
        parser.exit(message="Pass --run to start the standalone HTTP Ingress.\n")
    run_ingress(ROOT, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
