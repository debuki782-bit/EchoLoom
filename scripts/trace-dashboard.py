#!/usr/bin/env python
"""Run the local trace and tool management dashboard."""

import argparse

import uvicorn

from echoloom.trace_dashboard import create_trace_dashboard


def main() -> None:
    parser = argparse.ArgumentParser(description="启动本机 Agent 运行观察台")
    parser.add_argument("--port", type=int, default=8767)
    args = parser.parse_args()
    if not 1024 <= args.port <= 65535:
        parser.error("port must be between 1024 and 65535")
    uvicorn.run(create_trace_dashboard(), host="127.0.0.1", port=args.port, access_log=False)


if __name__ == "__main__":
    main()
