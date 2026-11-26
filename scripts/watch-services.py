"""One-stop launcher, self-healing health monitor, and live message/processing console."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
TMP_DIR = ROOT / ".tmp"
DATA_DIR = ROOT / "data"
START_PS1 = ROOT / "scripts" / "start-services.ps1"
HOOK_EXT_PS1 = ROOT / "scripts" / "hook-event-extension.ps1"
NATIVE_IMG_PS1 = ROOT / "scripts" / "native-image-download.ps1"
LIVE_PS1 = ROOT / "scripts" / "live.ps1"
PILOT_STDERR = TMP_DIR / "pilot.stderr.log"
INBOX_DB = DATA_DIR / "pilot-receipts.sqlite3"
MEMORY_DB = DATA_DIR / "agent-memory.sqlite3"
MONITOR_LOG = TMP_DIR / "service-monitor.log"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class Logger:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        need_bom = not path.exists() or path.stat().st_size == 0
        self.file = path.open("a", encoding="utf-8", buffering=1)
        if need_bom:
            self.file.write("\ufeff")
            self.file.flush()

    def log(self, tag: str, msg: str) -> None:
        line = f"[{_now()}] [{tag}] {msg}"
        print(line, flush=True)
        self.file.write(line + "\n")
        self.file.flush()

    def close(self) -> None:
        self.file.close()


def _compact(text: str, limit: int = 160) -> str:
    text = " ".join(str(text or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _format_inbound_payload(raw_json: str) -> str:
    try:
        payload = json.loads(raw_json)
    except (TypeError, ValueError):
        return _compact(raw_json)
    msg_type = payload.get("original_message_type") or payload.get("message_type")
    text = str(payload.get("text") or "")
    sender = str(payload.get("sender_id") or "")
    if sender and text.startswith(sender + ":\n"):
        text = text[len(sender) + 2 :]
    try:
        inner = json.loads(text)
    except (TypeError, ValueError):
        inner = None
    if isinstance(inner, dict):
        kind = inner.get("kind")
        if kind == "file_message":
            name = _compact(inner.get("filename") or inner.get("text") or "文件", 120)
            size_val = inner.get("size_bytes")
            size_text = f" ({size_val} B)" if isinstance(size_val, int) else ""
            return f"[文件] {name}{size_text}"
        if kind == "file_upload_notice":
            name = _compact(inner.get("filename") or inner.get("text") or "文件", 120)
            return f"[文件上传中] {name}"
        if kind == "app_message":
            title = _compact(inner.get("text") or "", 120)
            des = _compact(inner.get("description") or "", 80)
            url = str(inner.get("url") or "").split("?", 1)[0]
            return " | ".join(p for p in (f"卡片: {title}", des, url) if p)
        if kind == "quoted_message":
            return f"引用回复: {_compact(inner.get('text') or '', 120)}"
        if kind == "image_message":
            return "[表情/贴图]" if inner.get("media_kind") == "sticker" else "[图片]"
        if kind == "voice_message":
            return "[语音]"
        if kind == "pat_message":
            return f"[拍一拍] {inner.get('actor_id')} -> {inner.get('target_id')}"
    if msg_type == 49 and "<appmsg" in text:
        import xml.etree.ElementTree as ET

        start = text.find("<msg")
        if start >= 0:
            try:
                root = ET.fromstring(text[start:])
                app = root.find(".//appmsg")
                if app is not None:
                    title = (app.findtext("title") or "").strip()
                    url = (app.findtext("url") or "").strip().split("?", 1)[0]
                    des = _compact(app.findtext("des") or "", 80)
                    ref = app.find("refermsg")
                    if ref is not None:
                        return f"引用回复: {_compact(title, 100)}"
                    parts = [p for p in (f"卡片: {_compact(title, 120)}", des, url) if p]
                    return " | ".join(parts)
            except ET.ParseError:
                pass
    return f"type={msg_type} | {_compact(text)}"


def _hook_monitoring_enabled() -> bool:
    factory = (
        os.environ.get("ECHOLOOM_GATEWAY_FACTORY", "").strip()
        or os.environ.get("WECHAT_GATEWAY_FACTORY", "").strip()
    )
    if factory:
        return "hook" in factory.casefold()
    try:
        from echoloom import local_config

        default_id, specs = local_config.load_source_configs(ROOT)
        if specs:
            return any(spec.channel == "wechat" for spec in specs.values())
        return bool(local_config.load_local_runtime_config(ROOT).wechat_hook.launcher_exe)
    except Exception:
        return False


def _probe_services(client: httpx.Client) -> dict[str, tuple[bool, str]]:
    status: dict[str, tuple[bool, str]] = {}
    monitor_hook = _hook_monitoring_enabled()

    # 8767 Trace Dashboard (overview scans all traces, give it enough timeout)
    try:
        r = client.get("http://127.0.0.1:8767/api/overview", timeout=8.0)
        d = r.json()
        ok = r.status_code == 200 and "trace_records" in d
        status["8767"] = (ok, f"traces={d.get('trace_records')}" if ok else f"http={r.status_code}")
    except (httpx.HTTPError, ValueError, TypeError, KeyError) as exc:
        status["8767"] = (False, type(exc).__name__)

    if monitor_hook:
        # 30001 WeChat Hook
        try:
            r = client.get("http://127.0.0.1:30001/QueryDB/status")
            d = r.json()
            ok = r.status_code == 200 and d.get("IsLogin") == 1
            status["30001"] = (ok, f"login={d.get('IsLogin')},cb={d.get('MessageCallbackPosts')}")
        except (httpx.HTTPError, ValueError, TypeError, KeyError) as exc:
            status["30001"] = (False, type(exc).__name__)

        # 30002 Native Image Download
        try:
            r = client.get("http://127.0.0.1:30002/status")
            d = r.json()
            ok = r.status_code == 200 and d.get("service") == "wechat-native-image-download" and int(d.get("pid") or 0) > 0
            status["30002"] = (ok, f"cb_ready={d.get('callback_ready')},pid={d.get('pid')}")
        except (httpx.HTTPError, ValueError, TypeError, KeyError) as exc:
            status["30002"] = (False, type(exc).__name__)

        # 30003 Hook Event Extension (Bilibili long cards & pat)
        try:
            r = client.get("http://127.0.0.1:30003/status")
            d = r.json()
            ok = r.status_code == 200 and d.get("service") == "wechat-hook-event-extension" and bool(d.get("ready"))
            status["30003"] = (ok, f"rx={d.get('received')},tx={d.get('posted')},err={d.get('read_errors')}")
        except (httpx.HTTPError, ValueError, TypeError, KeyError) as exc:
            status["30003"] = (False, type(exc).__name__)

    # 8080 Agent Ingress + Worker
    try:
        r = client.get("http://127.0.0.1:8080/status")
        d = r.json()
        worker_ok = d.get("worker_alive") is not False
        login_watch_ok = d.get("login_watch_alive") is not False
        provider_ok = bool(d.get("provider_ready"))
        paused = bool(d.get("paused"))
        ok = (
            r.status_code == 200
            and d.get("mode") == "selfhosted_group"
            and provider_ok
            and not paused
            and worker_ok
            and login_watch_ok
        )
        extra_parts: list[str] = []
        if not worker_ok or not login_watch_ok:
            extra_parts.append(f"worker={worker_ok},login_watch={login_watch_ok}")
        if not provider_ok:
            extra_parts.append("provider=False")
        if paused:
            extra_parts.append("paused=True")
        if not d.get("trusted_gateway"):
            extra_parts.append("trusted=False")
        extra = ("," + ",".join(extra_parts)) if extra_parts else ""
        status["8080"] = (
            ok,
            f"q={d.get('queue')},rx={d.get('received')},inflight={bool(d.get('in_flight'))}{extra}",
        )
    except (httpx.HTTPError, ValueError, TypeError, KeyError) as exc:
        status["8080"] = (False, type(exc).__name__)

    return status


def _can_reach_gemini() -> bool:
    try:
        with httpx.Client(timeout=4.0, trust_env=True) as ext_client:
            ext_client.get("https://generativelanguage.googleapis.com/")
        return True
    except httpx.HTTPError:
        return False


def _heal_8080(
    st: dict[str, tuple[bool, str]],
    log: Logger,
    *,
    run_ps1=None,
    probe_gemini=None,
) -> str:
    """Self-heal 8080 via worker-only restart when Ingress is alive; full restart only if 8080 is down."""
    runner = run_ps1 or _run_ps1
    gemini_probe = probe_gemini or _can_reach_gemini
    ok_8080, detail_8080 = st.get("8080", (False, ""))
    if ok_8080:
        return "healthy"
    hook_logged_in = bool(st.get("30001", (False, ""))[0])
    ingress_alive = "q=" in detail_8080
    if ingress_alive:
        if "worker=False" in detail_8080 or "login_watch=False" in detail_8080:
            log.log(
                "自愈",
                f"8080 Ingress 存活但 Worker 异常 ({detail_8080})，仅重启 Worker（保持 :8080 不断链）……",
            )
            runner(LIVE_PS1, ["-Action", "restart-worker"], log)
            return "restart-worker"
        if hook_logged_in and ",trusted=False" in detail_8080:
            log.log(
                "自愈",
                f"8080 Ingress 存活但 Worker 信任态异常 ({detail_8080})，仅重启 Worker（保持 :8080 不断链）……",
            )
            runner(LIVE_PS1, ["-Action", "restart-worker"], log)
            return "restart-worker"
        if "provider=False" in detail_8080:
            if gemini_probe():
                log.log(
                    "自愈",
                    "8080 Ingress 存活但 provider_ready 持续异常且外网正常，仅重启 Worker 刷新连接池……",
                )
                runner(LIVE_PS1, ["-Action", "restart-worker"], log)
                return "restart-worker"
            return "skipped-gemini-unreachable"
        if "paused=True" in detail_8080:
            return "skipped-paused"
    log.log("自愈", f"8080 Ingress 不可达或状态异常 ({detail_8080})，执行完整重启……")
    runner(LIVE_PS1, ["-Action", "stop"], log)
    runner(LIVE_PS1, ["-Action", "start"], log)
    return "full-restart"


def _run_ps1(path: Path, args: list[str], log: Logger) -> int:
    # Do not capture stdout/stderr via pipes: detached background child processes
    # created by Start-Process can inherit pipe handles and hang subprocess.run.
    cmd = ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(path), *args]
    res = subprocess.run(cmd, check=False, cwd=str(ROOT))
    log.log("启动器", f"执行完成: {path.name} (exit={res.returncode})")
    return res.returncode


def _init_cursors() -> tuple[int, int, int]:
    inbox_rowid = 0
    if INBOX_DB.exists():
        with sqlite3.connect(f"file:{INBOX_DB}?mode=ro", uri=True) as conn:
            row = conn.execute("SELECT COALESCE(MAX(rowid), 0) FROM pilot_inbox").fetchone()
            inbox_rowid = int(row[0]) if row else 0

    mem_rowid = 0
    if MEMORY_DB.exists():
        with sqlite3.connect(f"file:{MEMORY_DB}?mode=ro", uri=True) as conn:
            row = conn.execute("SELECT COALESCE(MAX(rowid), 0) FROM source_messages").fetchone()
            mem_rowid = int(row[0]) if row else 0

    log_offset = PILOT_STDERR.stat().st_size if PILOT_STDERR.exists() else 0
    return inbox_rowid, mem_rowid, log_offset


def _poll_messages(inbox_rowid: int, mem_rowid: int, log_offset: int, log: Logger) -> tuple[int, int, int]:
    if INBOX_DB.exists():
        try:
            with sqlite3.connect(f"file:{INBOX_DB}?mode=ro", uri=True) as conn:
                rows = conn.execute(
                    "SELECT rowid, conversation_id, sender_id, state, payload_json "
                    "FROM pilot_inbox WHERE rowid > ? ORDER BY rowid ASC LIMIT 20",
                    (inbox_rowid,),
                ).fetchall()
                for rid, conv, sender, state, payload in rows:
                    inbox_rowid = max(inbox_rowid, int(rid))
                    summary = _format_inbound_payload(payload)
                    log.log("收到消息", f"{conv} | {sender} | state={state} | {summary}")
        except sqlite3.Error:
            pass

    if PILOT_STDERR.exists():
        try:
            size = PILOT_STDERR.stat().st_size
            if size < log_offset:
                log_offset = 0
            if size > log_offset:
                with PILOT_STDERR.open("rb") as f:
                    f.seek(log_offset)
                    chunk = f.read(size - log_offset).decode("utf-8", errors="replace")
                    log_offset = size
                for raw in chunk.splitlines():
                    if "QueryDB/status" in raw or "_ProactorBasePipeTransport._call_connection_lost" in raw:
                        continue
                    if "event=process_started" in raw:
                        log.log("处理中", raw.split("event=", 1)[1])
                    elif "event=adk_turn_started" in raw:
                        log.log("模型推理", raw.split("event=", 1)[1])
                    elif "event=adk_turn_finished" in raw:
                        log.log("推理完成", raw.split("event=", 1)[1])
                    elif "event=process_finished" in raw and "state=empty_reply" in raw:
                        log.log("决策结果", "本轮选择保持沉默 (" + raw.split("event=", 1)[1] + ")")
                    elif " ERROR " in raw or " CRITICAL " in raw:
                        log.log("异常", _compact(raw, 220))
        except OSError:
            pass

    if MEMORY_DB.exists():
        try:
            with sqlite3.connect(f"file:{MEMORY_DB}?mode=ro", uri=True) as conn:
                rows = conn.execute(
                    "SELECT rowid, conversation_id, direction, delivery_state, body_text "
                    "FROM source_messages WHERE rowid > ? AND direction = 'outbound' ORDER BY rowid ASC LIMIT 20",
                    (mem_rowid,),
                ).fetchall()
                for rid, conv, _dir, dstate, body in rows:
                    mem_rowid = max(mem_rowid, int(rid))
                    log.log("发出消息", f"-> {conv} | status={dstate or 'ok'} | {_compact(body, 180)}")
        except sqlite3.Error:
            pass

    return inbox_rowid, mem_rowid, log_offset


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-launch", action="store_true", help="Skip initial startup, monitor only.")
    parser.add_argument("--interval", type=int, default=15, help="Health check interval in seconds.")
    parser.add_argument("--cycles", type=int, default=0, help="Stop after N health cycles (0 = forever).")
    args = parser.parse_args()

    log = Logger(MONITOR_LOG)
    log.log("系统", f"服务常驻监控启动，日志文件：{MONITOR_LOG}")

    inbox_rowid, mem_rowid, log_offset = _init_cursors()

    if not args.no_launch:
        log.log("系统", "正在执行一站式启动与初始健康检查……")
        rc = _run_ps1(START_PS1, [], log)
        if rc != 0:
            log.log("警告", "初始启动存在异常项，将在后台循环中持续尝试自愈检查。")

    last_health_at = 0.0
    last_heal_at = 0.0
    last_heartbeat_at = 0.0
    prev_ok: dict[str, bool] = {}
    fail_streak: dict[str, int] = {}
    cycle = 0

    try:
        with httpx.Client(timeout=2.5, trust_env=False) as client:
            while True:
                now = time.monotonic()
                inbox_rowid, mem_rowid, log_offset = _poll_messages(inbox_rowid, mem_rowid, log_offset, log)

                if now - last_health_at >= args.interval:
                    last_health_at = now
                    cycle += 1
                    st = _probe_services(client)
                    cur_ok = {k: v[0] for k, v in st.items()}
                    if cur_ok.get("30001") and cur_ok.get("8080") and ",trusted=False" in st["8080"][1]:
                        cur_ok["8080"] = False
                        st["8080"] = (False, st["8080"][1])
                    failed = [k for k, ok in cur_ok.items() if not ok]
                    for k, ok in cur_ok.items():
                        fail_streak[k] = 0 if ok else fail_streak.get(k, 0) + 1
                    persistent_failed = [k for k in failed if fail_streak.get(k, 0) >= 2]

                    if cur_ok != prev_ok or (now - last_heartbeat_at >= 60):
                        last_heartbeat_at = now
                        summary = " | ".join(
                            f"{k}:{'OK' if ok else 'FAIL'}({detail})"
                            for k, (ok, detail) in st.items()
                        )
                        log.log("健康检查" if not failed else "健康告警", summary)
                        prev_ok = cur_ok

                    if persistent_failed and not args.no_launch and (now - last_heal_at >= 30):
                        last_heal_at = now
                        if "30001" in persistent_failed:
                            log.log("自愈", "30001 微信 Hook 离线或未登录，尝试拉起完整链路……")
                            _run_ps1(START_PS1, [], log)
                        else:
                            log.log("自愈", f"连续异常服务 {persistent_failed}，正在自动恢复……")
                            if "30002" in persistent_failed:
                                _run_ps1(NATIVE_IMG_PS1, ["-Action", "stop"], log)
                                _run_ps1(NATIVE_IMG_PS1, ["-Action", "start"], log)
                            if "30003" in persistent_failed:
                                _run_ps1(HOOK_EXT_PS1, ["-Action", "stop"], log)
                                _run_ps1(HOOK_EXT_PS1, ["-Action", "start"], log)
                            if "8080" in persistent_failed:
                                _heal_8080(st, log)
                            if "8767" in persistent_failed:
                                _run_ps1(START_PS1, [], log)

                    if args.cycles and cycle >= args.cycles:
                        break

                time.sleep(1.0)
    except KeyboardInterrupt:
        log.log("系统", "收到 Ctrl+C，监控窗口退出（已拉起的服务保持运行）。")
    finally:
        log.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
