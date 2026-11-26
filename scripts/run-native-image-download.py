"""Serve the native CDN extension on loopback; no AI or UI automation involved."""
from __future__ import annotations

import argparse
import hashlib
import json
import queue
import re
import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / ".runtime/native-image-download-deps"))
sys.path.insert(0, str(ROOT / ".tmp/native-image-analysis-deps"))

import httpx
from PIL import Image

from echoloom.native_image_download import MAX_BYTES, MD5

DLL_SHA = "f487e4f56930456a25fb0d7b058c77e2dcd364e268d49bdfff4dc0e3be26ba36"
HEX64 = re.compile(r"[0-9a-f]{64}")


class NativeAdapter:
    def __init__(self, pid: int, adapter: Path):
        import frida
        self.session = frida.attach(pid)
        self.detached = threading.Event()
        self.session.on("detached", lambda *_: self.detached.set())
        script = r'''
        const adapter = Module.load(ADAPTER);
        const optionalFn = (name, ret, args) => {
            const addr = adapter.findExportByName(name);
            return addr ? new NativeFunction(addr, ret, args) : null;
        };
        const init = new NativeFunction(adapter.getExportByName('ImageDownloadInitialize'), 'int', []);
        const ready = new NativeFunction(adapter.getExportByName('ImageDownloadReady'), 'int', []);
        const uploadReady = optionalFn('FileUploadReady', 'int', []);
        const submit = new NativeFunction(adapter.getExportByName('ImageDownloadSubmitKnown'),
            'int', ['pointer', 'pointer', 'pointer', 'uint', 'uint64']);
        const submitFileDl = optionalFn('FileDownloadSubmit',
            'int', ['pointer', 'pointer', 'pointer', 'pointer', 'uint', 'uint64']);
        const submitFileUl = optionalFn('FileUploadSubmit',
            'int', ['pointer', 'pointer', 'uint']);
        const popFileUl = optionalFn('FileUploadPopResult',
            'int', ['pointer', 'pointer', 'pointer', 'size_t', 'pointer', 'size_t', 'pointer']);
        const wx = Process.getModuleByName('Weixin.dll');
        const bound = new Map(), completed = new Map();
        function nativeString(p) {
            const n = p.add(16).readU64().toNumber();
            if (n > 128) return '';
            return (p.add(24).readU64().toNumber() < 16 ? p : p.readPointer()).readUtf8String(n);
        }
        // Observed ICdnCallback C2C download result, vtable slot 7. Observe and
        // forward it; never replace the callback or interpret submission as completion.
        Interceptor.attach(wx.base.add(0x21b32e0), {
            onEnter(args) {
                try {
                    const key = nativeString(args[1]);
                    if (bound.has(key)) completed.set(key, {
                        fileid: bound.get(key), code: args[2].readS32()
                    });
                } catch (_) {}
            }
        });
        rpc.exports = {
            initialize() { return init(); },
            ready() { return ready(); },
            uploadReady() { return uploadReady ? uploadReady() : 0; },
            submit(id, path, key, type, bytes) {
                bound.set(key, id);
                const result = submit(Memory.allocUtf8String(id), Memory.allocUtf8String(path),
                    Memory.allocUtf8String(key), type, bytes);
                if (result !== 0) { bound.delete(key); completed.delete(key); }
                return result;
            },
            submitFileDownload(id, aeskey, path, key, type, bytes) {
                if (!submitFileDl) return -1;
                bound.set(key, id);
                const result = submitFileDl(Memory.allocUtf8String(id), Memory.allocUtf8String(aeskey),
                    Memory.allocUtf8String(path), Memory.allocUtf8String(key), type, bytes);
                if (result !== 0) { bound.delete(key); completed.delete(key); }
                return result;
            },
            submitFileUpload(path, key, fileType) {
                if (!submitFileUl) return -1;
                return submitFileUl(Memory.allocUtf8String(path), Memory.allocUtf8String(key), fileType);
            },
            popFileUpload(key) {
                if (!popFileUl) return {completed: false, rc: -1};
                const outCode = Memory.alloc(4);
                const outBytes = Memory.alloc(8);
                const fileIdCap = 4096;
                const aesCap = 128;
                const outFileId = Memory.alloc(fileIdCap);
                const outAesKey = Memory.alloc(aesCap);
                const rc = popFileUl(Memory.allocUtf8String(key), outCode, outFileId, fileIdCap, outAesKey, aesCap, outBytes);
                if (rc !== 1) return {completed: false, rc: rc};
                return {
                    completed: true,
                    code: outCode.readS32(),
                    fileid: outFileId.readUtf8String(),
                    aeskey: outAesKey.readUtf8String(),
                    bytes: outBytes.readU64().toNumber(),
                };
            },
            result(key, id) {
                const result = completed.get(key);
                return result ? {completed: true, code: result.code, resource_bound: result.fileid === id}
                    : {completed: false};
            }
        };
        '''.replace("ADAPTER", json.dumps(str(adapter.resolve())))
        self.script = self.session.create_script(script)
        self.script.load()
        self.api = self.script.exports_sync
        result = self.api.initialize()
        if result != 0:
            self.close()
            raise RuntimeError(f"native extension initialization failed: {result}")

    def submit(self, spec, destination):
        return self.api.submit(spec["fileid"], str(destination),
                               "agent_image_" + spec["job_id"],
                               spec["image_type"], spec["expected_bytes"])

    def submit_file_download(self, spec, destination):
        return self.api.submit_file_download(
            spec["fileid"], spec["aeskey"], str(destination),
            "agent_file_" + spec["job_id"], spec["file_type"], spec["expected_bytes"],
        )

    def submit_file_upload(self, spec):
        return self.api.submit_file_upload(
            spec["file_path"], "agent_upload_" + spec["job_id"], spec["file_type"],
        )

    def pop_file_upload(self, spec):
        return self.api.pop_file_upload("agent_upload_" + spec["job_id"])

    def result(self, spec):
        prefix = "agent_file_" if "file_type" in spec else "agent_image_"
        return self.api.result(prefix + spec["job_id"], spec["fileid"])

    def close(self):
        if not self.detached.is_set():
            self.script.unload()
            self.session.detach()


HEX32 = re.compile(r"[0-9a-fA-F]{32}")


def _sha256_identity(spec: dict) -> str:
    identity = {k: v for k, v in spec.items() if k != "job_id"}
    return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


class DownloadService:
    def __init__(self, image_root: Path, adapter, account_id: str, account_check, *, file_root: Path | None = None):
        self.root = image_root.resolve()
        self.file_root = (
            file_root.resolve()
            if file_root is not None
            else (self.root.parent / "native-file-downloads").resolve()
        )
        self.adapter, self.account_id = adapter, account_id
        self.account_check = account_check
        self.root.mkdir(parents=True, exist_ok=True)
        self.file_root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.jobs = {}
        self.upload_ephemeral = {}
        self.pending = queue.Queue(maxsize=16)
        # Persist only operation receipts: no file IDs, AES keys or chat content.
        self.db = sqlite3.connect(self.root / "receipts.sqlite3", check_same_thread=False)
        self.db.execute("CREATE TABLE IF NOT EXISTS jobs(job_id TEXT PRIMARY KEY, state TEXT, reason TEXT)")
        if "metadata_json" not in {r[1] for r in self.db.execute("PRAGMA table_info(jobs)")}:
            self.db.execute("ALTER TABLE jobs ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}'")
        self.db.commit()
        self.worker = threading.Thread(target=self.work, daemon=True, name="native-image-download")
        self.worker.start()

    def close(self):
        self.pending.join()
        self.pending.put(None)
        self.worker.join(timeout=2)
        self.db.close()

    def validate(self, spec):
        if (not isinstance(spec, dict) or set(spec) != {
                "account_id", "fileid", "image_type", "expected_md5", "expected_bytes", "job_id"}
                or spec["account_id"] != self.account_id
                or not isinstance(spec["fileid"], str) or not 1 <= len(spec["fileid"]) <= 4096
                or "://" in spec["fileid"] or "\0" in spec["fileid"]
                or type(spec["image_type"]) is not int or spec["image_type"] not in (1, 2)
                or not isinstance(spec["expected_md5"], str) or not MD5.fullmatch(spec["expected_md5"])
                or type(spec["expected_bytes"]) is not int or not 0 <= spec["expected_bytes"] <= MAX_BYTES
                or not isinstance(spec["job_id"], str) or not HEX64.fullmatch(spec["job_id"])):
            raise ValueError("invalid_download_request")
        if _sha256_identity(spec) != spec["job_id"]:
            raise ValueError("download_identity_mismatch")

    def validate_file_download(self, spec):
        if (not isinstance(spec, dict) or set(spec) != {
                "account_id", "fileid", "aeskey", "file_type", "expected_md5", "expected_bytes", "job_id"}
                or spec["account_id"] != self.account_id
                or not isinstance(spec["fileid"], str) or not 1 <= len(spec["fileid"]) <= 4096
                or "://" in spec["fileid"] or "\0" in spec["fileid"]
                or not isinstance(spec["aeskey"], str) or not HEX32.fullmatch(spec["aeskey"])
                or type(spec["file_type"]) is not int or spec["file_type"] not in (5, 7)
                or not isinstance(spec["expected_md5"], str) or not MD5.fullmatch(spec["expected_md5"])
                or type(spec["expected_bytes"]) is not int or not 0 < spec["expected_bytes"] <= MAX_BYTES
                or not isinstance(spec["job_id"], str) or not HEX64.fullmatch(spec["job_id"])):
            raise ValueError("invalid_file_download_request")
        if _sha256_identity(spec) != spec["job_id"]:
            raise ValueError("download_identity_mismatch")

    def validate_file_upload(self, spec):
        if (not isinstance(spec, dict) or set(spec) != {
                "account_id", "file_path", "expected_md5", "expected_bytes", "file_type", "job_id"}
                or spec["account_id"] != self.account_id
                or not isinstance(spec["file_path"], str) or not 1 <= len(spec["file_path"]) <= 4096
                or "\0" in spec["file_path"]
                or type(spec["file_type"]) is not int or spec["file_type"] not in (5, 7)
                or not isinstance(spec["expected_md5"], str) or not MD5.fullmatch(spec["expected_md5"])
                or type(spec["expected_bytes"]) is not int or not 0 < spec["expected_bytes"] <= MAX_BYTES
                or not isinstance(spec["job_id"], str) or not HEX64.fullmatch(spec["job_id"])):
            raise ValueError("invalid_file_upload_request")
        if _sha256_identity(spec) != spec["job_id"]:
            raise ValueError("upload_identity_mismatch")

    def persist(self, job_id, state, reason="", **metadata):
        with self.lock:
            self.jobs[job_id] = {"job_id": job_id, "state": state, "reason": reason, **metadata}
            self.db.execute("INSERT INTO jobs(job_id,state,reason,metadata_json) VALUES (?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET state=excluded.state,reason=excluded.reason,metadata_json=excluded.metadata_json",
                            (job_id, state, reason, json.dumps(metadata)))
            self.db.commit()

    def get(self, job_id, *, include_ephemeral_upload: bool = False):
        with self.lock:
            if job_id in self.jobs:
                result = dict(self.jobs[job_id])
            else:
                row = self.db.execute("SELECT state,reason,metadata_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
                if not row:
                    return None
                state, reason, metadata = row
                if state in ("queued", "submitted"):
                    state, reason = "unknown", "service_restarted_during_download"
                result = {"job_id": job_id, "state": state, "reason": reason, **json.loads(metadata)}
            if include_ephemeral_upload and result.get("state") == "ready":
                secret = self.upload_ephemeral.get(job_id)
                if secret and time.monotonic() - secret["created_at"] < 1800:
                    result["fileid"] = secret["fileid"]
                    result["aeskey"] = secret["aeskey"]
                else:
                    return {"job_id": job_id, "state": "expired", "reason": "ephemeral_upload_credentials_expired"}
            return result

    def submit(self, spec, *, mode: str = "image"):
        if mode == "file_download":
            self.validate_file_download(spec)
        elif mode == "file_upload":
            self.validate_file_upload(spec)
        else:
            self.validate(spec)
        with self.lock:
            row = self.db.execute("SELECT state,reason,metadata_json FROM jobs WHERE job_id=?", (spec["job_id"],)).fetchone()
            if row and mode != "file_upload":
                state, reason, metadata = row
                active = self.jobs.get(spec["job_id"])
                retryable = state == "failed" and reason in ("native_result_-101", "native_result_-102")
                if active and not retryable:
                    return dict(active)
                if not retryable:
                    if state in ("queued", "submitted"):
                        state, reason = "unknown", "service_restarted_during_download"
                    return {"job_id": spec["job_id"], "state": state, "reason": reason, **json.loads(metadata)}
            if self.pending.full():
                raise ValueError("download_queue_full")
            result = {"job_id": spec["job_id"], "state": "queued", "reason": ""}
            self.db.execute("INSERT INTO jobs(job_id,state,reason) VALUES (?,?,?) ON CONFLICT(job_id) DO UPDATE SET state='queued',reason='',metadata_json='{}'",
                            (spec["job_id"], "queued", ""))
            self.db.commit()
            self.jobs[spec["job_id"]] = result
            queued_item = dict(spec)
            queued_item["_mode"] = mode
            self.pending.put_nowait(queued_item)
            return dict(result)

    def _work_upload(self, spec):
        job_id = spec["job_id"]
        src = Path(spec["file_path"])
        if not src.is_file():
            self.persist(job_id, "failed", "upload_source_not_found")
            return
        data = src.read_bytes()
        if len(data) != spec["expected_bytes"] or not 0 < len(data) <= MAX_BYTES:
            self.persist(job_id, "failed", "upload_size_mismatch")
            return
        if hashlib.md5(data).hexdigest() != spec["expected_md5"]:
            self.persist(job_id, "failed", "upload_md5_mismatch")
            return
        result = self.adapter.submit_file_upload(spec)
        deadline = time.monotonic() + 5
        while result == -101 and time.monotonic() < deadline:
            time.sleep(0.2)
            result = self.adapter.submit_file_upload(spec)
        if result != 0:
            state = "unknown" if result == -(2**31) else "failed"
            self.persist(job_id, state, "native_result_" + str(result))
            return
        self.persist(job_id, "submitted")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            completion = self.adapter.pop_file_upload(spec)
            if completion.get("completed"):
                code = completion.get("code", -1)
                if code != 0:
                    self.persist(job_id, "failed", "native_upload_completion_" + str(code))
                    return
                fileid = str(completion.get("fileid", ""))
                aeskey = str(completion.get("aeskey", "")).lower()
                if not fileid or not HEX32.fullmatch(aeskey):
                    self.persist(job_id, "failed", "native_upload_missing_credentials")
                    return
                with self.lock:
                    if len(self.upload_ephemeral) >= 64:
                        oldest = next(iter(self.upload_ephemeral))
                        del self.upload_ephemeral[oldest]
                    self.upload_ephemeral[job_id] = {
                        "fileid": fileid, "aeskey": aeskey, "created_at": time.monotonic(),
                    }
                self.persist(
                    job_id, "ready",
                    verification="file_md5",
                    sha256=hashlib.sha256(data).hexdigest(),
                    md5=spec["expected_md5"],
                    byte_count=len(data),
                    native_completion=0,
                )
                return
            time.sleep(0.2)
        self.persist(job_id, "unknown", "upload_completion_timeout")

    def work(self):
        while True:
            spec = self.pending.get()
            if spec is None:
                self.pending.task_done()
                return
            job_id = spec["job_id"]
            mode = spec.pop("_mode", "image")
            try:
                if not self.account_check():
                    self.persist(job_id, "failed", "wechat_account_changed_or_unavailable")
                    continue
                if mode == "file_upload":
                    self._work_upload(spec)
                    continue
                is_file = mode == "file_download"
                target_dir = self.file_root if is_file else self.root
                path = target_dir / (job_id + ".bin")
                if path.exists():
                    self.persist(job_id, "unknown", "unexpected_existing_output")
                    continue
                result = self.adapter.submit_file_download(spec, path) if is_file else self.adapter.submit(spec, path)
                # -102 has not submitted a native task. A just-arriving thumbnail
                # can supply credentials shortly after the inbound callback.
                deadline = time.monotonic() + 5
                while result == -102 and time.monotonic() < deadline:
                    time.sleep(0.2)
                    result = self.adapter.submit_file_download(spec, path) if is_file else self.adapter.submit(spec, path)
                if result != 0:
                    state = "unknown" if result == -(2**31) else "failed"
                    self.persist(job_id, state, "native_result_" + str(result))
                    continue
                self.persist(job_id, "submitted")
                deadline = time.monotonic() + (60 if is_file else 30)
                previous = None
                while time.monotonic() < deadline:
                    completion = self.adapter.result(spec)
                    if completion.get("completed") and completion.get("code") != 0:
                        self.persist(job_id, "failed", "native_completion_" + str(completion.get("code")))
                        break
                    if path.is_file():
                        stat = path.stat()
                        if stat.st_size > MAX_BYTES:
                            self.persist(job_id, "failed", "image_exceeds_download_limit")
                            break
                        fingerprint = (stat.st_size, stat.st_mtime_ns)
                        if stat.st_size and fingerprint == previous:
                            data = path.read_bytes()
                            exact = hashlib.md5(data).hexdigest() == spec["expected_md5"]
                            native_bound = (completion.get("completed") is True
                                            and completion.get("code") == 0
                                            and completion.get("resource_bound") is True)
                            if (exact and (not is_file or len(data) == spec["expected_bytes"])) or (not is_file and native_bound):
                                # CDN may return JPEG while WeChat cached the same
                                # resource as WXGF. Identity then comes from the
                                # native task's exact fileid/AES binding and success
                                # callback; validate file bytes independently.
                                if not is_file:
                                    with Image.open(path) as image:
                                        image.verify()
                                self.persist(job_id, "ready",
                                    verification="message_md5" if exact else "native_resource",
                                    resource_sha256=hashlib.sha256(spec["fileid"].encode()).hexdigest(),
                                    sha256=hashlib.sha256(data).hexdigest(), byte_count=len(data),
                                    native_completion=0 if native_bound else None)
                                break
                        previous = fingerprint
                    time.sleep(0.2)
                else:
                    self.persist(job_id, "unknown", "download_completion_or_md5_unconfirmed")
            except Exception as exc:
                self.persist(job_id, "unknown", "adapter_error_" + type(exc).__name__)
            finally:
                self.pending.task_done()


def current_account(account_id):
    try:
        with httpx.Client(timeout=2, trust_env=False) as client:
            status = client.get("http://127.0.0.1:30001/QueryDB/status").json()
            path = str(status.get("SqliteContactDbPath", "")).replace("\\", "/")
            return (status.get("IsLogin") == 1 and any(
                part == account_id or part.startswith(account_id + "_") for part in path.split("/")))
    except (httpx.HTTPError, ValueError, AttributeError):
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--account", required=True)
    parser.add_argument("--wechat-dll", type=Path, required=True)
    parser.add_argument("--adapter-dll", type=Path, default=ROOT / ".runtime/native-image-download/wx_image_download_411027.dll")
    parser.add_argument("--port", type=int, default=30002)
    args = parser.parse_args()
    if hashlib.sha256(args.wechat_dll.read_bytes()).hexdigest() != DLL_SHA:
        parser.error("unsupported Weixin.dll fingerprint")
    if not current_account(args.account):
        parser.error("the requested account is not currently logged in")
    adapter = NativeAdapter(args.pid, args.adapter_dll)
    service = DownloadService(
        ROOT / "data/native-image-downloads",
        adapter,
        args.account,
        lambda: current_account(args.account),
        file_root=ROOT / "data/native-file-downloads",
    )

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass  # HTTP paths and request bodies are private media metadata.

        def reply(self, status, result):
            payload = json.dumps(result).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path == "/status":
                try:
                    ready = bool(adapter.api.ready())
                    upload_ready = bool(adapter.api.upload_ready())
                except Exception:
                    ready = False
                    upload_ready = False
                self.reply(200, {"service": "wechat-native-image-download", "pid": args.pid,
                                 "callback_ready": ready, "upload_ready": upload_ready,
                                 "fingerprint": DLL_SHA})
            elif self.path.startswith("/Download_Image/"):
                job = self.path.removeprefix("/Download_Image/")
                result = service.get(job) if HEX64.fullmatch(job) else None
                self.reply(200 if result else 404, result or {"error": "job_not_found"})
            elif self.path.startswith("/Download_File/"):
                job = self.path.removeprefix("/Download_File/")
                result = service.get(job) if HEX64.fullmatch(job) else None
                self.reply(200 if result else 404, result or {"error": "job_not_found"})
            elif self.path.startswith("/Upload_File/"):
                job = self.path.removeprefix("/Upload_File/")
                result = service.get(job, include_ephemeral_upload=True) if HEX64.fullmatch(job) else None
                self.reply(200 if result else 404, result or {"error": "job_not_found"})
            else:
                self.reply(404, {"error": "not_found"})

        def do_POST(self):
            mode_map = {
                "/Download_Image": "image",
                "/Download_File": "file_download",
                "/Upload_File": "file_upload",
            }
            mode = mode_map.get(self.path)
            if mode is None:
                self.reply(404, {"error": "not_found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 10000:
                    raise ValueError("invalid_request_length")
                self.reply(200, service.submit(json.loads(self.rfile.read(length)), mode=mode))
            except (ValueError, TypeError, KeyError):
                self.reply(400, {"error": "invalid_request"})

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    def stop_after_wechat_exit():
        adapter.detached.wait()
        server.shutdown()

    threading.Thread(target=stop_after_wechat_exit, daemon=True).start()
    print(json.dumps({"event": "native_download_service_ready", "port": args.port,
                      "wechat_pid": args.pid}), flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()
        service.close()
        adapter.close()


if __name__ == "__main__":
    main()
