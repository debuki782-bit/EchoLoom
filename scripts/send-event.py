"""Send normalized events to EchoLoom's multi-source HTTP Ingress without WeChat."""

from __future__ import annotations

import sys

from echoloom.cli import send_main as main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
