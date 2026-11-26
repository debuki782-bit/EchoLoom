"""Windows Credential Manager storage for one model API key.

The command-line entry point reads from a masked prompt. Never pass a key as
an argument or print it. The gateway does not load this key yet.
"""

from __future__ import annotations

import argparse
import ctypes
import getpass
import os
from ctypes import wintypes

TARGET = "WeChatAIAgent/ModelAPI"
OPENAI_TARGET = "WeChatAIAgent/OpenAIAPI"
WEBHOOK_SECRET_TARGET = "EchoLoom/WebhookSecret"
_GENERIC = 1
_LOCAL_MACHINE = 2
_NOT_FOUND = 1168


class _Filetime(ctypes.Structure):
    _fields_ = [("low", wintypes.DWORD), ("high", wintypes.DWORD)]


class _Credential(ctypes.Structure):
    _fields_ = [
        ("Flags", wintypes.DWORD),
        ("Type", wintypes.DWORD),
        ("TargetName", wintypes.LPWSTR),
        ("Comment", wintypes.LPWSTR),
        ("LastWritten", _Filetime),
        ("CredentialBlobSize", wintypes.DWORD),
        ("CredentialBlob", ctypes.POINTER(ctypes.c_ubyte)),
        ("Persist", wintypes.DWORD),
        ("AttributeCount", wintypes.DWORD),
        ("Attributes", ctypes.c_void_p),
        ("TargetAlias", wintypes.LPWSTR),
        ("UserName", wintypes.LPWSTR),
    ]


def _api() -> ctypes.WinDLL:
    if os.name != "nt":
        raise OSError("Windows Credential Manager is required")
    library = ctypes.WinDLL("Advapi32.dll", use_last_error=True)
    library.CredWriteW.argtypes = [ctypes.POINTER(_Credential), wintypes.DWORD]
    library.CredWriteW.restype = wintypes.BOOL
    library.CredReadW.argtypes = [
        wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
        ctypes.POINTER(ctypes.POINTER(_Credential)),
    ]
    library.CredReadW.restype = wintypes.BOOL
    library.CredDeleteW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD]
    library.CredDeleteW.restype = wintypes.BOOL
    library.CredFree.argtypes = [ctypes.c_void_p]
    library.CredFree.restype = None
    return library


def store_key(secret: str, *, target: str = TARGET) -> None:
    if not secret or len(secret.encode("utf-8")) > 512:
        raise ValueError("key must be nonempty and at most 512 UTF-8 bytes")
    api = _api()
    data = secret.encode("utf-8")
    blob = ctypes.create_string_buffer(data)
    credential = _Credential()
    credential.Type = _GENERIC
    credential.TargetName = target
    credential.CredentialBlobSize = len(data)
    credential.CredentialBlob = ctypes.cast(blob, ctypes.POINTER(ctypes.c_ubyte))
    credential.Persist = _LOCAL_MACHINE
    credential.UserName = "Model API"
    try:
        if not api.CredWriteW(ctypes.byref(credential), 0):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        ctypes.memset(blob, 0, len(blob))


_ENV_HINTS = {
    TARGET: "GOOGLE_API_KEY",
    OPENAI_TARGET: "OPENAI_API_KEY",
    WEBHOOK_SECRET_TARGET: "ECHOLOOM_WEBHOOK_SECRET",
}


def load_key(*, target: str = TARGET) -> str | None:
    if os.name != "nt":
        return None
    api = _api()
    pointer = ctypes.POINTER(_Credential)()
    if not api.CredReadW(target, _GENERIC, 0, ctypes.byref(pointer)):
        error = ctypes.get_last_error()
        if error == _NOT_FOUND:
            return None
        raise ctypes.WinError(error)
    try:
        item = pointer.contents
        data = ctypes.string_at(item.CredentialBlob, item.CredentialBlobSize)
        return data.decode("utf-8")
    finally:
        api.CredFree(pointer)


def delete_key(*, target: str = TARGET) -> bool:
    api = _api()
    if api.CredDeleteW(target, _GENERIC, 0):
        return True
    error = ctypes.get_last_error()
    if error == _NOT_FOUND:
        return False
    raise ctypes.WinError(error)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Manage model API key without echoing it")
    parser.add_argument("action", choices=("store", "status", "delete"))
    parser.add_argument("--service", choices=("model", "openai", "webhook"), default="model")
    parser.add_argument("--target", default="", help="Optional custom credential target name")
    args = parser.parse_args(argv)
    action = args.action
    target = args.target.strip() or {
        "openai": OPENAI_TARGET,
        "model": TARGET,
        "webhook": WEBHOOK_SECRET_TARGET,
    }[args.service]
    if os.name != "nt":
        env_var = _ENV_HINTS.get(target, "appropriate environment variable")
        if action == "status":
            print(f"missing (Windows Credential Manager unavailable on {os.name}; set {env_var})")
            return 0
        print(
            f"Windows Credential Manager is unavailable on {os.name}. "
            f"Set {env_var} in your shell or .local/.env instead."
        )
        return 1
    if action == "store":
        secret = getpass.getpass("Secret value: ")
        store_key(secret, target=target)
        print("Credential stored for the current Windows user.")
    elif action == "status":
        print("stored" if load_key(target=target) is not None else "missing")
    else:
        print("deleted" if delete_key(target=target) else "missing")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
