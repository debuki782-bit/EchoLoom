"""Private local transport for the optional, version-specific CDN Hook extension."""
from __future__ import annotations

import ctypes
import hashlib
import json
import logging
import os
import re
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from PIL import Image

logger = logging.getLogger(__name__)

MAX_BYTES = 200_000_000
MD5 = re.compile(r"[0-9a-fA-F]{32}")
HEX32 = re.compile(r"[0-9a-fA-F]{32}")
_CDN_ATTACH_ID = re.compile(r"^@cdn_([0-9a-fA-F]{40,4096})_([0-9a-fA-F]{32})_\d+$")
SUPPORTED_WEIXIN_DLL_SHA256 = "f487e4f56930456a25fb0d7b058c77e2dcd364e268d49bdfff4dc0e3be26ba36"


def _validate_loopback_http_url(url: str, label: str) -> tuple[str, int]:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or parsed.username
        or parsed.password
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
        or parsed.port is None
    ):
        raise ValueError(f"{label} must be a loopback HTTP origin")
    return url.rstrip("/"), int(parsed.port)


def _loopback_listener_pids(port: int) -> set[int]:
    if os.name != "nt":
        return set()
    from ctypes import wintypes

    af_inet = 2
    tcp_table_owner_pid_listener = 3

    class _TcpRowOwnerPid(ctypes.Structure):
        _fields_ = [
            ("dwState", wintypes.DWORD),
            ("dwLocalAddr", wintypes.DWORD),
            ("dwLocalPort", wintypes.DWORD),
            ("dwRemoteAddr", wintypes.DWORD),
            ("dwRemotePort", wintypes.DWORD),
            ("dwOwningPid", wintypes.DWORD),
        ]

    iphlpapi = ctypes.WinDLL("iphlpapi", use_last_error=True)
    size = wintypes.DWORD(0)
    iphlpapi.GetExtendedTcpTable(
        None, ctypes.byref(size), False, af_inet, tcp_table_owner_pid_listener, 0,
    )
    if size.value <= 4:
        return set()
    buf = ctypes.create_string_buffer(size.value)
    ret = iphlpapi.GetExtendedTcpTable(
        buf, ctypes.byref(size), False, af_inet, tcp_table_owner_pid_listener, 0,
    )
    if ret != 0:
        return set()
    count = int(ctypes.cast(buf, ctypes.POINTER(wintypes.DWORD))[0])
    rows = ctypes.cast(ctypes.byref(buf, 4), ctypes.POINTER(_TcpRowOwnerPid * count)).contents
    expected_port = socket.htons(port)
    loopback_addr = int.from_bytes(socket.inet_aton("127.0.0.1"), "little")
    return {
        int(row.dwOwningPid)
        for row in rows
        if row.dwLocalPort == expected_port and row.dwLocalAddr in (loopback_addr, 0)
    }


def _weixin_dll_for_pid(pid: int) -> Path | None:
    if os.name != "nt" or pid <= 0:
        return None
    from ctypes import wintypes

    th32cs_snapmodule = 0x00000008
    th32cs_snapmodule32 = 0x00000010

    class _ModuleEntry32W(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD),
            ("th32ModuleID", wintypes.DWORD),
            ("th32ProcessID", wintypes.DWORD),
            ("GlblcntUsage", wintypes.DWORD),
            ("ProccntUsage", wintypes.DWORD),
            ("modBaseAddr", ctypes.c_void_p),
            ("modBaseSize", wintypes.DWORD),
            ("hModule", wintypes.HMODULE),
            ("szModule", wintypes.WCHAR * 256),
            ("szExePath", wintypes.WCHAR * 260),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    kernel32.Module32FirstW.argtypes = [wintypes.HANDLE, ctypes.POINTER(_ModuleEntry32W)]
    kernel32.Module32FirstW.restype = wintypes.BOOL
    kernel32.Module32NextW.argtypes = [wintypes.HANDLE, ctypes.POINTER(_ModuleEntry32W)]
    kernel32.Module32NextW.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]

    invalid_handle = ctypes.c_void_p(-1).value
    snap = kernel32.CreateToolhelp32Snapshot(th32cs_snapmodule | th32cs_snapmodule32, pid)
    if not snap or snap == invalid_handle:
        return None
    try:
        entry = _ModuleEntry32W()
        entry.dwSize = ctypes.sizeof(_ModuleEntry32W)
        if not kernel32.Module32FirstW(snap, ctypes.byref(entry)):
            return None
        exe_name = entry.szModule.casefold()
        if exe_name != "weixin.exe":
            return None
        dll_paths: list[Path] = []
        while True:
            if entry.szModule.casefold() == "weixin.dll" and entry.szExePath:
                dll_paths.append(Path(entry.szExePath))
            if not kernel32.Module32NextW(snap, ctypes.byref(entry)):
                break
        return dll_paths[0] if len(dll_paths) == 1 else None
    finally:
        kernel32.CloseHandle(snap)


def _service_healthy(download_url: str, expected_pid: int | None = None) -> bool:
    try:
        with httpx.Client(timeout=1.0, trust_env=False, follow_redirects=False) as client:
            resp = client.get(download_url + "/status")
        if resp.status_code != 200:
            return False
        data = resp.json()
        if (
            not isinstance(data, dict)
            or data.get("service") != "wechat-native-image-download"
            or not isinstance(data.get("pid"), int)
            or data["pid"] <= 0
        ):
            return False
        return expected_pid is None or data.get("pid") == expected_pid
    except (OSError, httpx.HTTPError, ValueError):
        return False


def _default_launch_native_service(
    workspace: Path, download_url: str, hook_url: str, account_id: str,
) -> bool:
    if os.name != "nt" or not account_id:
        return False
    download_origin, download_port = _validate_loopback_http_url(
        download_url, "native image download URL",
    )
    hook_origin, hook_port = _validate_loopback_http_url(hook_url, "Hook URL")

    hook_pids = _loopback_listener_pids(hook_port)
    if len(hook_pids) != 1:
        logger.warning(
            "event=native_image_download_autostart_skipped reason=hook_listener_not_unique count=%s",
            len(hook_pids),
        )
        return False
    weixin_pid = next(iter(hook_pids))

    if _service_healthy(download_origin, expected_pid=weixin_pid):
        return True

    existing_download_pids = _loopback_listener_pids(download_port)
    if existing_download_pids:
        logger.warning(
            "event=native_image_download_autostart_skipped reason=download_port_busy port=%s",
            download_port,
        )
        return False

    try:
        with httpx.Client(timeout=2.0, trust_env=False, follow_redirects=False) as client:
            status = client.get(hook_origin + "/QueryDB/status").json()
    except (OSError, httpx.HTTPError, ValueError) as exc:
        logger.warning(
            "event=native_image_download_autostart_skipped reason=hook_status_unavailable error=%s",
            type(exc).__name__,
        )
        return False

    contact_path = str(status.get("SqliteContactDbPath", "")).replace("\\", "/")
    if status.get("IsLogin") != 1 or not any(
        part == account_id or part.startswith(account_id + "_")
        for part in contact_path.split("/")
    ):
        logger.warning(
            "event=native_image_download_autostart_skipped reason=account_not_logged_in",
        )
        return False

    dll_path = _weixin_dll_for_pid(weixin_pid)
    if dll_path is None or not dll_path.is_file():
        logger.warning(
            "event=native_image_download_autostart_skipped reason=weixin_dll_not_found pid=%s",
            weixin_pid,
        )
        return False
    try:
        dll_sha = hashlib.sha256(dll_path.read_bytes()).hexdigest()
    except OSError:
        return False
    if dll_sha != SUPPORTED_WEIXIN_DLL_SHA256:
        logger.warning(
            "event=native_image_download_autostart_skipped reason=unsupported_weixin_dll_sha",
        )
        return False

    script_path = (workspace / "scripts" / "run-native-image-download.py").resolve()
    adapter_dll = (
        workspace / ".runtime" / "native-image-download" / "wx_image_download_411027.dll"
    ).resolve()
    if not script_path.is_file() or not adapter_dll.is_file():
        logger.warning(
            "event=native_image_download_autostart_skipped reason=extension_files_missing",
        )
        return False

    venv_python = workspace / ".venv" / "Scripts" / "python.exe"
    python_exe = venv_python if venv_python.is_file() else Path(sys.executable)
    tmp_dir = workspace / ".tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = tmp_dir / "native-image-download.stdout.log"
    stderr_path = tmp_dir / "native-image-download.stderr.log"

    flags = (
        getattr(subprocess, "DETACHED_PROCESS", 0)
        | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        | getattr(subprocess, "CREATE_NO_WINDOW", 0)
    )
    cmd = [
        str(python_exe),
        str(script_path),
        "--pid",
        str(weixin_pid),
        "--account",
        account_id,
        "--wechat-dll",
        str(dll_path),
        "--adapter-dll",
        str(adapter_dll),
        "--port",
        str(download_port),
    ]
    try:
        with stdout_path.open("ab") as out_f, stderr_path.open("ab") as err_f:
            proc = subprocess.Popen(
                cmd,
                cwd=str(workspace),
                stdin=subprocess.DEVNULL,
                stdout=out_f,
                stderr=err_f,
                close_fds=True,
                creationflags=flags,
            )
    except OSError as exc:
        logger.warning(
            "event=native_image_download_autostart_failed reason=spawn_error error=%s",
            type(exc).__name__,
        )
        return False

    deadline = time.monotonic() + 8.0
    while time.monotonic() < deadline:
        if _service_healthy(download_origin, expected_pid=weixin_pid):
            logger.info(
                "event=native_image_download_autostarted port=%s wechat_pid=%s service_pid=%s",
                download_port,
                weixin_pid,
                proc.pid,
            )
            return True
        if proc.poll() is not None:
            break
        time.sleep(0.25)

    logger.warning(
        "event=native_image_download_autostart_failed reason=ready_timeout port=%s wechat_pid=%s",
        download_port,
        weixin_pid,
    )
    return False


def download_spec(account_id: str, references: list[dict[str, str]]) -> dict | None:
    values = {r.get("field"): r.get("value", "") for r in references}
    # Message MD5 normally identifies the regular full image (native type 2).
    # Request sender-original/type 1 only with its own exact hash and resource.
    hd = values.get("cdnhdimgurl") or values.get("hdfileid")
    if hd and MD5.fullmatch(values.get("hdmd5", "")):
        fileid, digest, image_type, length = hd, values["hdmd5"], 1, values.get("hdlength", "0")
    else:
        fileid = values.get("cdnmidimgurl") or values.get("fileid") or ""
        digest, image_type, length = values.get("md5", ""), 2, values.get("length", "0")
    if (not fileid or len(fileid) > 4096 or "://" in fileid
            or not MD5.fullmatch(digest) or not length.isdigit()):
        return None
    if not 0 <= int(length) <= MAX_BYTES:
        return None
    spec = {"account_id": account_id, "fileid": fileid, "image_type": image_type,
            "expected_md5": digest.lower(), "expected_bytes": int(length)}
    spec["job_id"] = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
    return spec


def file_download_spec(account_id: str, references: list[dict[str, str]]) -> dict | None:
    """Build deterministic C2C file-download request matching :30002 validation."""
    values = {r.get("field"): r.get("value", "") for r in references}
    fileid = values.get("cdnattachurl", "").strip()
    aeskey = values.get("aeskey", "").strip().lower()
    attachid = values.get("attachid", "").strip()
    if (not fileid or not aeskey) and attachid:
        matched = _CDN_ATTACH_ID.fullmatch(attachid)
        if matched:
            fileid = fileid or matched.group(1)
            aeskey = aeskey or matched.group(2).lower()
    digest = values.get("md5", "").strip().lower()
    length = values.get("totallen", "").strip()
    if (not account_id or not fileid or len(fileid) > 4096
            or "://" in fileid or "\0" in fileid
            or not HEX32.fullmatch(aeskey)
            or not MD5.fullmatch(digest)
            or not length.isdigit()):
        return None
    expected_bytes = int(length)
    if not 0 < expected_bytes <= MAX_BYTES:
        return None
    file_type = 7 if expected_bytes > 25 * 1024 * 1024 else 5
    spec = {
        "account_id": account_id,
        "fileid": fileid,
        "aeskey": aeskey,
        "file_type": file_type,
        "expected_md5": digest,
        "expected_bytes": expected_bytes,
    }
    spec["job_id"] = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
    return spec


def file_upload_spec(
    account_id: str,
    file_path: Path,
    expected_md5: str,
    byte_count: int,
) -> dict | None:
    """Build deterministic C2C file-upload request matching :30002 validation."""
    path_str = str(file_path)
    if (not account_id or not path_str or len(path_str) > 4096 or "\0" in path_str
            or not MD5.fullmatch(expected_md5)
            or not 0 < int(byte_count) <= MAX_BYTES):
        return None
    file_type = 7 if int(byte_count) > 25 * 1024 * 1024 else 5
    spec = {
        "account_id": account_id,
        "file_path": path_str,
        "expected_md5": expected_md5.lower(),
        "expected_bytes": int(byte_count),
        "file_type": file_type,
    }
    spec["job_id"] = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
    return spec


class NativeImageDownloader:
    def __init__(
        self,
        workspace: Path,
        url: str,
        *,
        transport=None,
        hook_url: str = "http://127.0.0.1:30001",
        auto_start: bool = False,
        service_launcher: Callable[[str], bool] | None = None,
        autostart_cooldown_seconds: float = 15.0,
    ):
        self.workspace = Path(workspace)
        self.url, _ = _validate_loopback_http_url(url, "native image download URL")
        self.hook_url, self._hook_port = _validate_loopback_http_url(hook_url, "Hook URL")
        self.output_root = (self.workspace / "data/native-image-downloads").resolve()
        self.file_output_root = (self.workspace / "data/native-file-downloads").resolve()
        self.transport = transport
        self.auto_start = bool(auto_start and transport is None)
        self._service_launcher = service_launcher
        self._autostart_cooldown_seconds = max(0.0, float(autostart_cooldown_seconds))
        self._launch_lock = threading.Lock()
        self._last_failed_launch_monotonic: float | None = None

    def _expected_hook_pid(self) -> int | None:
        if os.name != "nt":
            return None
        pids = _loopback_listener_pids(self._hook_port)
        return next(iter(pids)) if len(pids) == 1 else None

    def ensure_running(self, account_id: str) -> bool:
        if not account_id:
            return False
        expected_pid = self._expected_hook_pid()
        if self._service_launcher is None and not self.auto_start:
            return _service_healthy(self.url, expected_pid) if self.transport is None else False
        if self._service_launcher is None and _service_healthy(self.url, expected_pid):
            return True
        with self._launch_lock:
            expected_pid = self._expected_hook_pid()
            if self._service_launcher is None and _service_healthy(self.url, expected_pid):
                return True
            now = time.monotonic()
            if (
                self._last_failed_launch_monotonic is not None
                and now - self._last_failed_launch_monotonic < self._autostart_cooldown_seconds
            ):
                return False
            launcher = self._service_launcher or (
                lambda acc: _default_launch_native_service(
                    self.workspace, self.url, self.hook_url, acc,
                )
            )
            try:
                started = bool(launcher(account_id))
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "event=native_image_download_launcher_error error=%s",
                    type(exc).__name__,
                )
                started = False
            if started:
                self._last_failed_launch_monotonic = None
            else:
                self._last_failed_launch_monotonic = time.monotonic()
            return started

    def _post_and_poll(
        self,
        client: httpx.Client,
        spec: dict,
        *,
        post_path: str = "/Download_Image",
        poll_prefix: str = "/Download_Image/",
        max_wait_seconds: float = 38.0,
    ) -> dict | None:
        response = client.post(self.url + post_path, json=spec)
        if response.status_code != 200:
            logger.warning(
                "event=native_transfer_http_error path=%s status=%s",
                post_path, response.status_code,
            )
            return None
        result = response.json()
        deadline = time.monotonic() + max_wait_seconds
        while result.get("state") in ("queued", "submitted") and time.monotonic() < deadline:
            time.sleep(0.2)
            response = client.get(self.url + poll_prefix + spec["job_id"])
            if response.status_code != 200:
                logger.warning(
                    "event=native_transfer_poll_http_error path=%s status=%s",
                    poll_prefix, response.status_code,
                )
                return None
            result = response.json()
        return result if isinstance(result, dict) else None

    def _submit_job(
        self,
        account_id: str,
        spec: dict,
        *,
        post_path: str,
        poll_prefix: str,
        max_wait_seconds: float = 38.0,
    ) -> dict | None:
        with httpx.Client(timeout=3, trust_env=False, follow_redirects=False,
                          transport=self.transport) as client:
            try:
                return self._post_and_poll(
                    client,
                    spec,
                    post_path=post_path,
                    poll_prefix=poll_prefix,
                    max_wait_seconds=max_wait_seconds,
                )
            except httpx.ConnectError:
                if (self.auto_start or self._service_launcher is not None) and self.ensure_running(
                    account_id,
                ):
                    return self._post_and_poll(
                        client,
                        spec,
                        post_path=post_path,
                        poll_prefix=poll_prefix,
                        max_wait_seconds=max_wait_seconds,
                    )
                logger.warning(
                    "event=native_image_download_unreachable url=%s", self.url,
                )
                return None

    def fetch(self, account_id: str, references: list[dict[str, str]], destination: Path) -> bool:
        spec = download_spec(account_id, references)
        if spec is None:
            return False
        try:
            result = self._submit_job(
                account_id,
                spec,
                post_path="/Download_Image",
                poll_prefix="/Download_Image/",
            )
            if result is None:
                return False
            # A timed-out/restarted native task must never be resubmitted,
            # but its eventual file can still be used after exact hashing.
            if (
                result.get("state") not in ("ready", "unknown")
                or result.get("job_id") != spec["job_id"]
            ):
                logger.warning(
                    "event=native_image_download_not_ready state=%s reason=%s",
                    result.get("state"),
                    result.get("reason", ""),
                )
                return False
            # Resolve our own deterministic path. Never trust a path supplied
            # by a process answering on the local port.
            source = (self.output_root / (spec["job_id"] + ".bin")).resolve()
            if not source.is_relative_to(self.output_root) or not source.is_file():
                return False
            if not 0 < source.stat().st_size <= MAX_BYTES:
                return False
            payload = source.read_bytes()
            exact = hashlib.md5(payload).hexdigest() == spec["expected_md5"]
            native_bound = (
                result.get("state") == "ready"
                and result.get("verification") == "native_resource"
                and type(result.get("native_completion")) is int
                and result.get("native_completion") == 0
                and result.get("resource_sha256")
                == hashlib.sha256(spec["fileid"].encode()).hexdigest()
                and result.get("sha256") == hashlib.sha256(payload).hexdigest()
                and result.get("byte_count") == len(payload)
            )
            if not exact and not native_bound:
                logger.warning("event=native_image_download_verification_failed")
                return False
            if native_bound:
                with Image.open(source) as image:
                    image.verify()
            destination.write_bytes(payload)
            return True
        except (OSError, httpx.HTTPError, ValueError, TypeError, AttributeError) as exc:
            logger.warning(
                "event=native_image_download_failed error_type=%s", type(exc).__name__,
            )
            return False

    def fetch_file(self, account_id: str, references: list[dict[str, str]], destination: Path) -> bool:
        spec = file_download_spec(account_id, references)
        if spec is None:
            return False
        try:
            result = self._submit_job(
                account_id,
                spec,
                post_path="/Download_File",
                poll_prefix="/Download_File/",
                max_wait_seconds=68.0,
            )
            if result is None:
                return False
            if (
                result.get("state") not in ("ready", "unknown")
                or result.get("job_id") != spec["job_id"]
            ):
                logger.warning(
                    "event=native_file_download_not_ready state=%s reason=%s",
                    result.get("state"),
                    result.get("reason", ""),
                )
                return False
            source = (self.file_output_root / (spec["job_id"] + ".bin")).resolve()
            if not source.is_relative_to(self.file_output_root) or not source.is_file():
                return False
            size = source.stat().st_size
            if size != spec["expected_bytes"] or not 0 < size <= MAX_BYTES:
                return False
            digest = hashlib.md5(usedforsecurity=False)
            with source.open("rb") as f:
                while chunk := f.read(1024 * 1024):
                    digest.update(chunk)
            if digest.hexdigest() != spec["expected_md5"]:
                logger.warning("event=native_file_download_md5_mismatch")
                return False
            destination.parent.mkdir(parents=True, exist_ok=True)
            with source.open("rb") as src, destination.open("wb") as dst:
                while chunk := src.read(1024 * 1024):
                    dst.write(chunk)
            return True
        except (OSError, httpx.HTTPError, ValueError, TypeError, AttributeError) as exc:
            logger.warning(
                "event=native_file_download_failed error_type=%s", type(exc).__name__,
            )
            return False

    def upload_file(self, account_id: str, path: Path) -> tuple[str, str, str, int] | None:
        try:
            if not path.is_file():
                return None
            digest = hashlib.md5(usedforsecurity=False)
            size = 0
            with path.open("rb") as f:
                while chunk := f.read(1024 * 1024):
                    digest.update(chunk)
                    size += len(chunk)
                    if size > MAX_BYTES:
                        return None
            if size <= 0:
                return None
            expected_md5 = digest.hexdigest()
            spec = file_upload_spec(account_id, path, expected_md5, size)
            if spec is None:
                return None
            result = self._submit_job(
                account_id,
                spec,
                post_path="/Upload_File",
                poll_prefix="/Upload_File/",
                max_wait_seconds=68.0,
            )
            if (not result or result.get("state") != "ready"
                    or result.get("job_id") != spec["job_id"]):
                return None
            fileid = str(result.get("fileid", "")).strip()
            aeskey = str(result.get("aeskey", "")).strip().lower()
            if not fileid or not HEX32.fullmatch(aeskey):
                return None
            return fileid, aeskey, expected_md5, size
        except (OSError, httpx.HTTPError, ValueError, TypeError, AttributeError) as exc:
            logger.warning(
                "event=native_file_upload_failed error_type=%s", type(exc).__name__,
            )
            return None
