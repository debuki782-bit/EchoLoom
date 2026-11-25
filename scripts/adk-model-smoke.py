"""One explicitly enabled synthetic ADK tool test; no WeChat data or sends."""

import argparse
import asyncio
import json
from pathlib import Path

from google import genai
from google.adk.models.google_llm import Gemini
from google.genai import types

from wechat_agent.adk_budget import BudgetedAdkModel
from wechat_agent.adk_runner import ScopedAdkRunner
from wechat_agent.agent_tools import AgentToolStatus
from wechat_agent.context import ContextTurn, ContextWindow
from wechat_agent.credentials import load_key
from wechat_agent.mock import SystemClock
from wechat_agent.storage import Repository
from wechat_agent.usage import UsageLedger

ROOT = Path(__file__).resolve().parents[1]


async def run() -> None:
    repo = Repository(ROOT / "data" / "model-smoke-usage.sqlite3")
    if repo.conn.execute(
        "SELECT count(*) FROM usage_reservations WHERE feature='adk_chat'"
    ).fetchone()[0]:
        repo.close()
        raise SystemExit("ADK model attempt already recorded; no automatic repeat.")
    secret = load_key()
    if not secret:
        repo.close()
        raise SystemExit("Credential missing.")
    marker = ROOT / ".tmp" / "adk-smoke-attempt.json"
    marker.parent.mkdir(exist_ok=True)
    with marker.open("x", encoding="utf-8") as stream:
        json.dump({"started_at": SystemClock().now()}, stream)
    http_options = types.HttpOptions(
        timeout=20000, retry_options=types.HttpRetryOptions(attempts=1),
    )
    token_client = genai.Client(api_key=secret, http_options=http_options)
    delegate = Gemini(model="gemini-3.5-flash-lite", client_kwargs={
        "api_key": secret,
        "http_options": http_options,
    })
    model = BudgetedAdkModel(delegate, UsageLedger(repo, SystemClock()), SystemClock(),
                             model_id=delegate.model, budget_microusd=20000,
                             token_counter=token_client.aio.models.count_tokens)
    runner = ScopedAdkRunner(model=model, status_tool=AgentToolStatus())
    result: dict[str, object] = {}
    try:
        text = await runner.reply(ContextWindow(
            "synthetic-test-account", "synthetic-adk-chat", False,
            (ContextTurn("synthetic-sender", "synthetic_test",
                         "这是工具连接测试。请调用 read_gateway_status 查询状态，"
                         "然后仅回答自动发送是否启用。"),),
        ))
        result["response_present"] = bool(text)
        sessions = await runner.sessions.list_sessions(app_name="wechat_agent_mock")
        events = [event for session in sessions.sessions for event in (
            (await runner.sessions.get_session(app_name="wechat_agent_mock",
                user_id=session.user_id, session_id=session.id)).events
        )]
        result["tool_calls"] = sum(
            len(event.get_function_calls()) for event in events
        )
        result["tool_results"] = sum(
            len(event.get_function_responses()) for event in events
        )
    except Exception as exc:  # noqa: BLE001 - emit type only, never provider secrets
        result["error_type"] = type(exc).__name__
    finally:
        await runner.close()
        result["usage"] = [dict(row) for row in repo.conn.execute(
            "SELECT state,reserved_microusd,actual_microusd FROM usage_reservations "
            "WHERE feature='adk_chat'"
        )]
        repo.close()
        marker.write_text(json.dumps(result), encoding="utf-8")
    print(json.dumps(result))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="store_true")
    arguments = parser.parse_args()
    if not arguments.run:
        parser.exit(message="Pass --run for one budgeted synthetic API test.\n")
    asyncio.run(run())
