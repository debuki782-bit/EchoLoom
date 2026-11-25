"""Scheme A dual-process WAL IPC, standalone Ingress isolation, and worker self-heal tests."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
from fastapi.testclient import TestClient

from wechat_agent.conversation_scheduler import ConversationScheduler
from wechat_agent.dispatcher import Dispatcher
from wechat_agent.ingress import Ingress, create_ingress_app, sha
from wechat_agent.models import ReplyDraft
from wechat_agent.pilot import Pilot
from wechat_agent.wechat_gateway import MessageEvent, SendResult

ROOT = Path(__file__).resolve().parents[1]
ACCOUNT = "wxid_bot_scheme_a"
ROOM = "55555555555@chatroom"
SENDER = "wxid_friend_scheme_a"


def _load_script_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _hook_group_payload(
    *,
    text: str,
    source_msg_id: int,
    sender: str = SENDER,
    room: str = ROOM,
    account: str = ACCOUNT,
) -> dict[str, object]:
    return {
        "sendorrecv": 2,
        "selfwxid": account,
        "msglist": [
            {
                "fromid": room,
                "toid": account,
                "room": room,
                "sender": sender,
                "msg": f"{sender}:\n{text}",
                "msgtype": 1,
                "msgsvrid": source_msg_id,
                "time": 1_700_000_000_000 + source_msg_id,
            }
        ],
    }


def _hook_private_payload(
    *,
    text: str,
    source_msg_id: int,
    sender: str = SENDER,
    account: str = ACCOUNT,
) -> dict[str, object]:
    return {
        "sendorrecv": 2,
        "selfwxid": account,
        "msglist": [
            {
                "fromid": sender,
                "toid": account,
                "room": "",
                "sender": sender,
                "msg": text,
                "msgtype": 1,
                "msgsvrid": source_msg_id,
                "time": 1_700_000_000_000 + source_msg_id,
            }
        ],
    }


def test_run_ingress_imports_zero_llm_sdks(tmp_path: Path, monkeypatch) -> None:
    """Importing and building scripts/run-ingress.py must pull zero LLM SDK modules."""
    probe_code = (
        "import importlib.util, json, sys\n"
        "from pathlib import Path\n"
        "script = Path(sys.argv[1])\n"
        "spec = importlib.util.spec_from_file_location('run_ingress_probe', script)\n"
        "mod = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(mod)\n"
        "forbidden = ('google.adk', 'google.genai', 'openai', 'agents', "
        "'wechat_agent.episode_memory', 'wechat_agent.session_runtime')\n"
        "found = sorted(m for m in sys.modules if any(m == f or m.startswith(f + '.') for f in forbidden))\n"
        "print(json.dumps(found))\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", probe_code, str(ROOT / "scripts" / "run-ingress.py")],
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(completed.stdout.strip()) == []

    # Also verify CLI --run wires standalone Ingress with recover_running=False and WAL.
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "live-route.json").write_text(
        json.dumps({"account": ACCOUNT, "room": ROOM}),
        encoding="utf-8",
    )
    run_ingress = _load_script_module("run_ingress_cli_test", "scripts/run-ingress.py")
    captured: dict[str, object] = {}

    def fake_uvicorn_run(app, **kwargs):
        captured["app"] = app
        captured["kwargs"] = kwargs

    monkeypatch.setattr(run_ingress, "ROOT", tmp_path)
    monkeypatch.setattr(run_ingress.uvicorn, "run", fake_uvicorn_run)
    run_ingress.main(["--run"])

    assert captured["kwargs"]["host"] == "127.0.0.1"
    assert captured["kwargs"]["port"] == 8080
    with TestClient(captured["app"], client=("127.0.0.1", 50001)) as client:
        status = client.get("/status")
        assert status.status_code == 200
        body = status.json()
        assert body["mode"] == "selfhosted_group"
        assert body["worker_alive"] is False
        assert body["queue"] == 0

    with sqlite3.connect(tmp_path / "data" / "pilot-receipts.sqlite3") as conn:
        journal_mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
    assert journal_mode == "wal"


def test_scheme_a_dual_wal_ipc_callback_heartbeat_inflight_and_completion(tmp_path: Path) -> None:
    """Real dual SQLite WAL connections: Ingress /callback+/status + independent Worker loop."""
    db_path = tmp_path / "pilot-receipts.sqlite3"
    ingress_db = sqlite3.connect(db_path, check_same_thread=False)
    ingress = Ingress(
        ingress_db,
        ACCOUNT,
        ROOM,
        mode="selfhosted_group",
        limit=None,
        heartbeat_max_age_seconds=5.0,
        recover_running=False,
    )
    app = create_ingress_app(ingress)

    # Verify exact /status key parity with Pilot.status() before Worker connects.
    ref_pilot = Pilot(
        tmp_path / "ref-pilot.sqlite3",
        ACCOUNT,
        ROOM,
        AsyncMock(return_value=""),
        lambda _draft: SendResult("queued"),
        mode="selfhosted_group",
        limit=None,
    )
    try:
        assert set(ingress.status().keys()) == set(ref_pilot.status().keys())
    finally:
        ref_pilot.db.close()

    reply_entered = threading.Event()
    release_reply = threading.Event()
    worker_started = threading.Event()
    sent_drafts: list[ReplyDraft] = []
    worker_stop: dict[str, object] = {}

    def run_worker_thread() -> None:
        async def runner() -> None:
            worker_db = sqlite3.connect(db_path)
            worker_db.executescript(
                "CREATE TABLE IF NOT EXISTS attempts (id TEXT PRIMARY KEY,state TEXT);"
                "CREATE TABLE IF NOT EXISTS control (id INTEGER PRIMARY KEY,paused INTEGER);"
                "INSERT OR IGNORE INTO control VALUES (1,0);"
            )
            worker_db.commit()
            scheduler = ConversationScheduler(
                worker_db,
                debounce_seconds=0.05,
                max_wait_seconds=0.1,
                recover_running=True,
            )

            async def fake_reply(_window) -> str:
                reply_entered.set()
                await asyncio.to_thread(release_reply.wait, 5.0)
                return "收到，这是独立 Worker 的回复"

            def fake_send(draft: ReplyDraft) -> SendResult:
                sent_drafts.append(draft)
                return SendResult("queued")

            dispatcher = Dispatcher(
                worker_db,
                scheduler,
                ACCOUNT,
                ROOM,
                fake_reply,
                fake_send,
                limit=None,
                is_paused=lambda: bool(
                    worker_db.execute("SELECT paused FROM control WHERE id=1").fetchone()[0]
                ),
                is_wechat_ready=lambda: True,
                heartbeat_interval_seconds=0.2,
            )
            task = asyncio.create_task(dispatcher.run_worker())
            worker_stop["loop"] = asyncio.get_running_loop()
            worker_stop["task"] = task
            worker_started.set()
            try:
                await task
            except asyncio.CancelledError:
                pass
            finally:
                worker_db.close()

        asyncio.run(runner())

    thread = threading.Thread(target=run_worker_thread, daemon=True)
    try:
        with TestClient(app, client=("127.0.0.1", 50002)) as client:
            before = client.get("/status").json()
            assert before["worker_alive"] is False
            assert before["trusted_gateway"] is False
            assert before["provider_ready"] is False
            assert before["in_flight"] is None
            assert before["queue"] == 0

            thread.start()
            assert worker_started.wait(3.0)

            deadline = time.monotonic() + 3.0
            alive_status = client.get("/status").json()
            while not alive_status["worker_alive"] and time.monotonic() < deadline:
                time.sleep(0.05)
                alive_status = client.get("/status").json()
            assert alive_status["worker_alive"] is True
            assert alive_status["trusted_gateway"] is True
            assert alive_status["provider_ready"] is True
            assert alive_status["in_flight"] is None

            # Deliver callback through Ingress HTTP; Worker claims and starts replying.
            cb = client.post(
                "/callback",
                json=_hook_group_payload(text="双进程通信测试", source_msg_id=91001),
            )
            assert cb.status_code == 200
            assert cb.json() == {"accepted": 1, "results": ["queued_test"]}
            assert reply_entered.wait(5.0)

            # While Worker is mid-turn, /status must expose worker_alive, in_flight, and queue=1.
            mid = client.get("/status").json()
            assert mid["worker_alive"] is True
            assert mid["queue"] == 1
            assert mid["in_flight"] is not None
            assert mid["in_flight"]["sender_sha"] == sha(SENDER)
            assert mid["in_flight"]["message_type"] == 1
            assert mid["in_flight"]["elapsed_seconds"] >= 0.0

            # Restarting Ingress (recover_running=False) mid-turn must NEVER clobber running row!
            restarted_ingress = Ingress(
                db_path,
                ACCOUNT,
                ROOM,
                mode="selfhosted_group",
                recover_running=False,
            )
            try:
                assert restarted_ingress.scheduler.state_counts().get("running") == 1
            finally:
                restarted_ingress.close()

            # Let Worker finish and confirm completion in Ingress /status.
            release_reply.set()
            deadline = time.monotonic() + 5.0
            done = client.get("/status").json()
            while (done["queue"] != 0 or done["in_flight"] is not None) and time.monotonic() < deadline:
                time.sleep(0.05)
                done = client.get("/status").json()

            assert done["worker_alive"] is True
            assert done["queue"] == 0
            assert done["in_flight"] is None
            assert done["inbox_states"].get("model_seen") == 1
            assert len(sent_drafts) == 1
            assert sent_drafts[0].text == "收到，这是独立 Worker 的回复"

            # Pause via /ai off through Ingress updates control table visible to Worker DB.
            pause_cb = client.post(
                "/callback",
                json=_hook_private_payload(text="/ai off", source_msg_id=91002),
            )
            assert pause_cb.status_code == 200
            assert pause_cb.json()["results"] == ["paused"]
            assert client.get("/status").json()["paused"] is True
    finally:
        release_reply.set()
        loop = worker_stop.get("loop")
        task = worker_stop.get("task")
        if isinstance(loop, asyncio.AbstractEventLoop) and isinstance(task, asyncio.Task):
            loop.call_soon_threadsafe(task.cancel)
        if thread.is_alive():
            thread.join(timeout=5.0)
        ingress_db.close()


def test_scheme_a_concurrent_begin_immediate_batch_claim_is_exclusive(tmp_path: Path) -> None:
    """Two independent worker connections racing BEGIN IMMEDIATE claim must never double-claim."""
    db_path = tmp_path / "concurrent-claim.sqlite3"
    ingress = Ingress(db_path, ACCOUNT, ROOM, mode="selfhosted_group", recover_running=False)
    try:
        for idx in range(3):
            event = MessageEvent(
                account_id=ACCOUNT,
                conversation_id=ROOM,
                sender_id=SENDER,
                sender_origin="hook_group_member_wxid",
                text=f"并发认领测试消息 #{idx}",
                is_group=True,
                message_type=1,
                source_msg_id=92000 + idx,
                capture_tick_ms=1_700_000_000_000 + idx,
            )
            assert ingress.offer(event) == "queued_test"
    finally:
        ingress.close()

    barrier = threading.Barrier(2)
    claim_time = datetime.now(UTC) + timedelta(seconds=30)

    def attempt_claim() -> str | None:
        conn = sqlite3.connect(db_path)
        try:
            sched = ConversationScheduler(conn, debounce_seconds=0.05, max_wait_seconds=0.1, recover_running=False)
            barrier.wait(timeout=3.0)
            batch = sched.claim_ready_batch(now=claim_time)
            return batch.batch_id if batch is not None else None
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: attempt_claim(), range(2)))

    winners = [batch_id for batch_id in results if batch_id is not None]
    assert len(winners) == 1
    assert results.count(None) == 1


def test_scheme_a_stale_heartbeat_marks_worker_dead_while_ingress_still_enqueues(tmp_path: Path) -> None:
    """When Worker heartbeat expires, /status reports worker_alive=False while /callback stays live."""
    db_path = tmp_path / "stale-hb.sqlite3"
    worker_conn = sqlite3.connect(db_path)
    try:
        worker_sched = ConversationScheduler(worker_conn, recover_running=False)
        worker_sched.update_worker_heartbeat(
            worker_id="default",
            pid=43210,
            wechat_ready=True,
            provider_ready=True,
            in_flight={"sender_sha": "abc", "message_type": 1, "elapsed_seconds": 3.0},
            scheduled_followups=2,
            now=datetime.now(UTC) - timedelta(seconds=30),
        )
    finally:
        worker_conn.close()

    ingress_db = sqlite3.connect(db_path, check_same_thread=False)
    ingress = Ingress(
        ingress_db,
        ACCOUNT,
        ROOM,
        mode="selfhosted_group",
        heartbeat_max_age_seconds=5.0,
        recover_running=False,
    )
    try:
        with TestClient(create_ingress_app(ingress), client=("127.0.0.1", 50003)) as client:
            st = client.get("/status").json()
            assert st["worker_alive"] is False
            assert st["login_watch_alive"] is False
            assert st["trusted_gateway"] is False
            assert st["provider_ready"] is False
            assert st["in_flight"] is None
            assert st["scheduled_followups"] == 0

            # Ingress still durably accepts callbacks while Worker is dead/restarting!
            cb = client.post(
                "/callback",
                json=_hook_group_payload(text="Worker 重启期间不丢消息", source_msg_id=93001),
            )
            assert cb.status_code == 200
            assert cb.json() == {"accepted": 1, "results": ["queued_test"]}
            assert client.get("/status").json()["queue"] == 1
    finally:
        ingress_db.close()


def test_run_group_pilot_cli_roles_ingress_and_worker(tmp_path: Path, monkeypatch) -> None:
    """Verify --role ingress (zero-LLM path) and --role worker (no :8080 HTTP bind, skips legacy recovery)."""
    (tmp_path / "data").mkdir()
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "agent_profiles.toml").write_text(
        (ROOT / "config" / "agent_profiles.toml").read_text("utf-8"),
        encoding="utf-8",
    )
    (tmp_path / "data" / "live-route.json").write_text(
        json.dumps({"account": ACCOUNT, "room": ROOM}),
        encoding="utf-8",
    )

    startup = _load_script_module("run_group_pilot_roles_test", "scripts/run-group-pilot.py")

    # 1. --role ingress: must not require API keys or instantiate ScopedAgentRuntime/WeChatTools.
    ingress_uvicorn: dict[str, object] = {}

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("LLM/Agent runtime must not be instantiated in --role ingress")

    monkeypatch.setattr(startup, "ROOT", tmp_path)
    monkeypatch.setattr(startup, "load_key", lambda **_kwargs: None)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setattr(startup, "ScopedAgentRuntime", fail_if_called)
    monkeypatch.setattr(startup, "WeChatTools", fail_if_called)
    monkeypatch.setattr(
        startup.uvicorn,
        "run",
        lambda app, **kwargs: ingress_uvicorn.update({"app": app, "kwargs": kwargs}),
    )
    monkeypatch.setattr(sys, "argv", ["run-group-pilot.py", "--run", "--role", "ingress"])
    startup.main()

    assert ingress_uvicorn["kwargs"]["port"] == 8080
    with TestClient(ingress_uvicorn["app"], client=("127.0.0.1", 50004)) as client:
        assert client.get("/status").json()["mode"] == "selfhosted_group"

    # 2. --role worker: must NOT bind HTTP :8080, wires OutboundService, skips redundant login recovery.
    worker_tmp = tmp_path / "worker_case"
    (worker_tmp / "data").mkdir(parents=True)
    (worker_tmp / "config").mkdir()
    (worker_tmp / "config" / "agent_profiles.toml").write_text(
        (ROOT / "config" / "agent_profiles.toml").read_text("utf-8"),
        encoding="utf-8",
    )
    (worker_tmp / "data" / "live-route.json").write_text(
        json.dumps({"account": ACCOUNT, "room": ROOM}),
        encoding="utf-8",
    )

    startup_w = _load_script_module("run_group_pilot_worker_test", "scripts/run-group-pilot.py")
    captured_worker: dict[str, object] = {}
    runtimes: list[object] = []
    toolsets: list[object] = []
    clients: list[httpx.Client] = []
    real_tools = startup_w.WeChatTools
    real_http = httpx.Client

    def capture_tools(*args, **kwargs):
        value = real_tools(*args, **kwargs)
        toolsets.append(value)
        return value

    def hook(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "IsLogin": 1,
                "SqliteContactDbPath": f"C:/xwechat_files/{ACCOUNT}_abc/db_storage/contact.db",
            },
        )

    def offline_http(*args, **kwargs):
        client = real_http(*args, **{**kwargs, "transport": httpx.MockTransport(hook)})
        clients.append(client)
        return client

    def capture_runtime(database_path, **kwargs):
        rt = SimpleNamespace(
            database_path=database_path,
            model_id=kwargs["model"].model,
            register_conversation=AsyncMock(),
            close=AsyncMock(),
        )
        runtimes.append(rt)
        return rt

    def fail_uvicorn(*_args, **_kwargs):
        raise AssertionError("uvicorn.run must NOT be called in --role worker")

    def capture_worker_loop(pilot, **kwargs):
        captured_worker["pilot"] = pilot
        captured_worker["kwargs"] = kwargs

    monkeypatch.setattr(startup_w, "ROOT", worker_tmp)
    monkeypatch.setattr(startup_w, "load_key", lambda **_kwargs: "offline-key")
    monkeypatch.setenv("OPENAI_API_KEY", "offline-key")
    monkeypatch.setenv("WECHAT_AGENT_EPISODE_RAG", "0")
    monkeypatch.setattr(startup_w, "ScopedAgentRuntime", capture_runtime)
    monkeypatch.setattr(startup_w, "WeChatTools", capture_tools)
    monkeypatch.setattr(startup_w.httpx, "Client", offline_http)
    monkeypatch.setattr(
        startup_w.genai,
        "Client",
        lambda **_: SimpleNamespace(
            aio=SimpleNamespace(models=SimpleNamespace(count_tokens=AsyncMock()))
        ),
    )
    monkeypatch.setattr(startup_w.uvicorn, "run", fail_uvicorn)
    monkeypatch.setattr(startup_w, "run_worker_loop", capture_worker_loop)
    monkeypatch.setattr(sys, "argv", ["run-group-pilot.py", "--run", "--role", "worker"])
    try:
        startup_w.main()
        pilot = captured_worker["pilot"]
        assert isinstance(pilot, Pilot)
        assert pilot.dispatcher.outbound is toolsets[0].outbound
        assert pilot.time_scheduler is not None

        # Verify initialize_after_login skips redundant archived/latest recovery in Scheme A worker mode.
        archived_called = False
        latest_called = False
        failed_called = False

        def mark_archived(**_kwargs):
            nonlocal archived_called
            archived_called = True
            return {"queued": 0}

        def mark_latest(_event):
            nonlocal latest_called
            latest_called = True
            return "ok"

        def mark_failed(**_kwargs):
            nonlocal failed_called
            failed_called = True
            return {"queued": 0}

        monkeypatch.setattr(pilot, "recover_archived_backlog", mark_archived)
        monkeypatch.setattr(pilot, "recover_latest_event", mark_latest)
        monkeypatch.setattr(pilot, "recover_failed_after_login", mark_failed)
        monkeypatch.setattr(pilot.dispatcher.image_ingestor, "ensure_native_downloader", lambda _acc: True)
        asyncio.run(pilot.triggers.login_watcher.initialize_after_login())
        assert archived_called is False
        assert latest_called is False
        assert failed_called is True
    finally:
        if "pilot" in captured_worker:
            captured_worker["pilot"].db.close()
            if captured_worker["pilot"].memory_store is not None:
                captured_worker["pilot"].memory_store.close()
        for t in toolsets:
            t.db.close()
        for c in clients:
            c.close()


def test_watch_services_heals_worker_without_restarting_ingress(tmp_path: Path) -> None:
    """When :8080 Ingress is responsive, self-healing must use restart-worker and never stop Ingress."""
    watch = _load_script_module("watch_services_test", "scripts/watch-services.py")
    log = watch.Logger(tmp_path / "monitor.log")
    try:
        # 1. Worker dead, Ingress alive -> restart-worker only.
        calls: list[tuple[str, list[str]]] = []

        def fake_run(path: Path, args: list[str], _log):
            calls.append((path.name, list(args)))
            return 0

        action = watch._heal_8080(
            {
                "30001": (True, "login=1,cb=10"),
                "8080": (False, "q=2,rx=5,inflight=False,worker=False,login_watch=False,provider=False,trusted=False"),
            },
            log,
            run_ps1=fake_run,
            probe_gemini=lambda: True,
        )
        assert action == "restart-worker"
        assert calls == [("live.ps1", ["-Action", "restart-worker"])]

        # 2. Worker alive but trusted=False while 30001 is logged in -> restart-worker only.
        calls.clear()
        action = watch._heal_8080(
            {
                "30001": (True, "login=1,cb=10"),
                "8080": (False, "q=0,rx=5,inflight=False,trusted=False"),
            },
            log,
            run_ps1=fake_run,
            probe_gemini=lambda: True,
        )
        assert action == "restart-worker"
        assert calls == [("live.ps1", ["-Action", "restart-worker"])]

        # 3. Worker alive, provider_ready=False, external Gemini reachable -> restart-worker only.
        calls.clear()
        action = watch._heal_8080(
            {
                "30001": (True, "login=1,cb=10"),
                "8080": (False, "q=1,rx=5,inflight=False,provider=False"),
            },
            log,
            run_ps1=fake_run,
            probe_gemini=lambda: True,
        )
        assert action == "restart-worker"
        assert calls == [("live.ps1", ["-Action", "restart-worker"])]

        # 4. Intentionally paused (worker/provider/trusted OK) -> do NOT restart.
        calls.clear()
        action = watch._heal_8080(
            {
                "30001": (True, "login=1,cb=10"),
                "8080": (False, "q=0,rx=5,inflight=False,paused=True"),
            },
            log,
            run_ps1=fake_run,
            probe_gemini=lambda: True,
        )
        assert action == "skipped-paused"
        assert calls == []

        # 5. :8080 Ingress itself unreachable -> full stop + start.
        calls.clear()
        action = watch._heal_8080(
            {
                "30001": (True, "login=1,cb=10"),
                "8080": (False, "ConnectError"),
            },
            log,
            run_ps1=fake_run,
            probe_gemini=lambda: True,
        )
        assert action == "full-restart"
        assert calls == [
            ("live.ps1", ["-Action", "stop"]),
            ("live.ps1", ["-Action", "start"]),
        ]
    finally:
        log.close()
