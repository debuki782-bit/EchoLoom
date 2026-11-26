"""Bridge naturally received long cards and pat notifications to the Hook callback."""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import queue
import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / ".runtime/native-image-download-deps"))

import httpx

from echoloom.adapters.hook_event_extension import DLL_SHA, PROBE, event_envelope


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", required=True, type=int)
    parser.add_argument("--account", required=True)
    parser.add_argument("--wechat-dll", required=True, type=Path)
    parser.add_argument("--observe-only", action="store_true")
    args = parser.parse_args()
    if hashlib.sha256(args.wechat_dll.read_bytes()).hexdigest() != DLL_SHA:
        parser.error("unsupported Weixin.dll fingerprint")

    import frida
    incoming = queue.Queue(maxsize=256)
    stopped = threading.Event()
    ready = threading.Event()
    counters = {"received": 0, "posted": 0, "rejected": 0, "read_errors": 0, "queue_full": 0}
    evidence = ROOT / ".tmp/hook-event-extension-observed.jsonl"
    database = ROOT / "data/hook-event-outbox.sqlite3"
    database.parent.mkdir(parents=True, exist_ok=True)
    tick = ctypes.WinDLL("kernel32").GetTickCount64
    tick.restype = ctypes.c_ulonglong

    def message_received(message, _data):
        event = message.get("payload") if message.get("type") == "send" else None
        if not isinstance(event, dict):
            counters["read_errors"] += 1
            return
        if event.get("event") == "ready":
            ready.set()
            return
        if event.get("event") == "read_error":
            counters["read_errors"] += 1
            return
        counters["received"] += 1
        try:
            incoming.put_nowait((event, int(tick())))
        except queue.Full:
            counters["queue_full"] += 1

    def work():
        with sqlite3.connect(database) as db, httpx.Client(timeout=5, trust_env=False) as client:
            db.execute("CREATE TABLE IF NOT EXISTS outbox (id TEXT PRIMARY KEY, payload TEXT, created REAL)")
            db.commit()
            while not stopped.is_set() or not incoming.empty():
                try:
                    event, capture_tick = incoming.get(timeout=.25)
                except queue.Empty:
                    event = None
                if event is not None:
                    if args.observe_only:
                        evidence.parent.mkdir(parents=True, exist_ok=True)
                        with evidence.open("a", encoding="utf-8") as output:
                            output.write(json.dumps(event, ensure_ascii=False) + "\n")
                    else:
                        envelope = event_envelope(event, args.account, capture_tick)
                        if envelope is None:
                            counters["rejected"] += 1
                        else:
                            payload = json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))
                            source_id = envelope["msglist"][0]["msgsvrid"]
                            # An exact native server ID survives redelivery and service restart.
                            identity = f"{args.account}:{event['from']}:{source_id}" if source_id else payload
                            digest = hashlib.sha256(identity.encode()).hexdigest()
                            if len(payload.encode()) > 131072:
                                counters["rejected"] += 1
                            else:
                                db.execute("INSERT OR IGNORE INTO outbox VALUES (?,?,?)",
                                           (digest, payload, time.time()))
                                db.commit()
                    incoming.task_done()
                if args.observe_only or stopped.is_set():
                    continue
                row = db.execute("SELECT id,payload FROM outbox WHERE payload IS NOT NULL ORDER BY created LIMIT 1").fetchone()
                if row:
                    try:
                        response = client.post("http://127.0.0.1:8080/callback",
                                               content=row[1].encode(), headers={"Content-Type": "application/json"})
                        if response.status_code == 200:
                            counters["posted"] += 1
                            db.execute("UPDATE outbox SET payload=NULL WHERE id=?", (row[0],))
                            db.commit()
                        elif response.status_code in (400, 413):
                            counters["rejected"] += 1
                            db.execute("UPDATE outbox SET payload=NULL WHERE id=?", (row[0],))
                            db.commit()
                        else:
                            stopped.wait(1)
                    except httpx.HTTPError:
                        stopped.wait(1)
                db.execute("DELETE FROM outbox WHERE payload IS NULL AND created < ?", (time.time() - 7 * 86400,))
                db.commit()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != "/stop":
                self.send_error(404)
                return
            if self.headers.get("X-Local-Control") != "stop":
                self.send_error(403)
                return
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            stopped.set()

        def do_GET(self):
            if self.path != "/status":
                self.send_error(404)
                return
            data = json.dumps({"service": "wechat-hook-event-extension", "pid": args.pid,
                               "ready": ready.is_set() and not stopped.is_set(),
                               "observe_only": args.observe_only,
                               "queue_depth": incoming.qsize(), **counters}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 30003), Handler)
    session = frida.attach(args.pid)
    session.on("detached", lambda *_: stopped.set())
    script = session.create_script(PROBE)
    script.on("message", message_received)
    worker = None
    serving = False
    try:
        script.load()
        if not ready.wait(5):
            raise RuntimeError("receive observer initialization failed")
        worker = threading.Thread(target=work, daemon=True)
        worker.start()
        threading.Thread(target=server.serve_forever, daemon=True).start()
        serving = True
        while not stopped.wait(.5):
            if not worker.is_alive():
                raise RuntimeError("callback worker stopped")
    finally:
        stopped.set()
        try:
            script.unload()
            session.detach()
        except frida.InvalidOperationError:
            pass
        if worker is not None:
            worker.join(timeout=6)
        if serving:
            server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
