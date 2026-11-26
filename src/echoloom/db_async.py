"""Thread-safe async offloading helpers for SQLite-backed stores."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
import weakref
from collections.abc import Callable

_REGISTRY_LOCK = threading.Lock()
_THREAD_SAFE_LOCKS: dict[int, threading.RLock] = {}


def _unregister_db_id(db_id: int) -> None:
    with _REGISTRY_LOCK:
        _THREAD_SAFE_LOCKS.pop(db_id, None)


def unregister_thread_safe_db(db: sqlite3.Connection | None) -> None:
    """Remove a SQLite connection from the cross-thread safety registry."""
    if db is not None:
        _unregister_db_id(id(db))


def register_thread_safe_db(
    db: sqlite3.Connection,
    lock: threading.RLock | None = None,
    *,
    owner: object | None = None,
) -> threading.RLock:
    """Register a ``check_same_thread=False`` connection and return its shared RLock."""
    db_id = id(db)
    with _REGISTRY_LOCK:
        existing = _THREAD_SAFE_LOCKS.get(db_id)
        if existing is not None:
            return existing
        resolved = lock or threading.RLock()
        _THREAD_SAFE_LOCKS[db_id] = resolved
    if owner is not None:
        weakref.finalize(owner, _unregister_db_id, db_id)
    return resolved


def is_db_thread_safe(db: sqlite3.Connection | None) -> bool:
    """Return True when ``db`` is registered as safe for cross-thread access."""
    if db is None:
        return False
    with _REGISTRY_LOCK:
        return id(db) in _THREAD_SAFE_LOCKS


def db_lock(db: sqlite3.Connection | None) -> threading.RLock | None:
    """Return the RLock associated with a registered connection, if any."""
    if db is None:
        return None
    with _REGISTRY_LOCK:
        return _THREAD_SAFE_LOCKS.get(id(db))


def _call_locked[T](
    lock: threading.RLock | None,
    fn: Callable[..., T],
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> T:
    if lock is None:
        return fn(*args, **kwargs)
    with lock:
        return fn(*args, **kwargs)


async def run_db[T](
    fn: Callable[..., T],
    *args: object,
    db: sqlite3.Connection | None = None,
    lock: threading.RLock | None = None,
    **kwargs: object,
) -> T:
    """Run synchronous SQLite work off the event loop when the connection is thread-safe.

    Injected connections in unit tests often use SQLite's default
    ``check_same_thread=True``; unregistered connections execute inline to
    preserve caller-thread affinity, whereas EchoLoom-managed connections execute
    via ``asyncio.to_thread`` serialized by their per-connection ``RLock``.
    """
    effective_lock = lock or db_lock(db)
    if db is None or is_db_thread_safe(db):
        return await asyncio.to_thread(_call_locked, effective_lock, fn, args, kwargs)
    return _call_locked(effective_lock, fn, args, kwargs)
