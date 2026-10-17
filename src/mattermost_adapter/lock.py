from __future__ import annotations

import fcntl
import os
from pathlib import Path

from .errors import AlreadyRunningError


def lock_path_for_state(state_path: str) -> str | None:
    if not state_path or state_path == ":memory:" or state_path.startswith("file:"):
        return None
    return f"{state_path}.lock"


def acquire_listener_lock(state_path: str) -> int | None:
    """Durum dosyası başına tek dinleyici kilidi.

    `fcntl.flock` süreç ölünce (SIGKILL dahil) çekirdek tarafından bırakılır.
    Kilit dosyası `<durum>.lock` yolundadır. Kilit alınamazsa ağa hiç
    çıkılmadan `AlreadyRunningError` yükseltilir.
    """
    path = lock_path_for_state(state_path)
    if path is None:
        return None
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        os.close(descriptor)
        raise AlreadyRunningError(f"another listener already holds {path}") from exc
    return descriptor


def release_listener_lock(descriptor: int | None) -> None:
    if descriptor is None:
        return
    try:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)
