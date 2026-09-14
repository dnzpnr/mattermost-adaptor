from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Protocol


class StateStore(Protocol):
    def list_channels(self) -> list[str]: ...
    def get_last_post_id(self, channel_id: str) -> str | None: ...
    def is_post_processed(self, post_id: str) -> bool: ...
    def initialize_cursor(self, channel_id: str, post_id: str, create_at: int = 0) -> None: ...
    def mark_post_processed(self, channel_id: str, post_id: str, create_at: int = 0) -> None: ...


class SqliteStateStore:
    def __init__(self, path: str | os.PathLike[str], channels: tuple[str, ...] | list[str] = ()) -> None:
        self.path = str(path)
        self._lock = threading.RLock()
        self._memory_connection = (sqlite3.connect(":memory:", check_same_thread=False)
                                   if self.path == ":memory:" else None)
        if self.path != ":memory:" and not self.path.startswith("file:"):
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                os.chmod(self.path, 0o600)
            else:
                os.fchmod(fd, 0o600)
                os.close(fd)
        with self._connect() as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS configured_channels (channel_id TEXT PRIMARY KEY)")
            connection.execute("CREATE TABLE IF NOT EXISTS channel_state (channel_id TEXT PRIMARY KEY, last_post_id TEXT NOT NULL, last_post_create_at INTEGER NOT NULL DEFAULT 0)")
            connection.execute("CREATE TABLE IF NOT EXISTS processed_posts (post_id TEXT PRIMARY KEY, channel_id TEXT NOT NULL, create_at INTEGER NOT NULL DEFAULT 0)")
            connection.execute("CREATE INDEX IF NOT EXISTS ix_processed_posts_channel ON processed_posts (channel_id, create_at)")
            connection.execute("DELETE FROM configured_channels")
            connection.executemany("INSERT OR IGNORE INTO configured_channels(channel_id) VALUES (?)",
                                   ((channel,) for channel in channels))

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            connection = self._memory_connection or sqlite3.connect(
                self.path, uri=self.path.startswith("file:"), timeout=30)
            try:
                yield connection
                connection.commit()
            finally:
                if self._memory_connection is None:
                    connection.close()

    def close(self) -> None:
        with self._lock:
            if self._memory_connection is not None:
                self._memory_connection.close()
                self._memory_connection = None

    def list_channels(self) -> list[str]:
        with self._connect() as connection:
            return [row[0] for row in connection.execute("SELECT channel_id FROM configured_channels ORDER BY channel_id")]

    def get_last_post_id(self, channel_id: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute("SELECT last_post_id FROM channel_state WHERE channel_id=?", (channel_id,)).fetchone()
        return row[0] if row else None

    def is_post_processed(self, post_id: str) -> bool:
        with self._connect() as connection:
            return connection.execute("SELECT 1 FROM processed_posts WHERE post_id=?", (post_id,)).fetchone() is not None

    def initialize_cursor(self, channel_id: str, post_id: str, create_at: int = 0) -> None:
        if not channel_id:
            raise ValueError("channel_id cannot be empty")
        with self._connect() as connection:
            connection.execute("INSERT OR IGNORE INTO channel_state(channel_id,last_post_id,last_post_create_at) VALUES(?,?,?)",
                               (channel_id, post_id, int(create_at or 0)))

    def mark_post_processed(self, channel_id: str, post_id: str, create_at: int = 0) -> None:
        if not channel_id or not post_id:
            raise ValueError("channel_id and post_id cannot be empty")
        timestamp = int(create_at or 0)
        with self._connect() as connection:
            connection.execute("INSERT OR IGNORE INTO processed_posts(post_id,channel_id,create_at) VALUES(?,?,?)",
                               (post_id, channel_id, timestamp))
            connection.execute(
                "INSERT INTO channel_state(channel_id,last_post_id,last_post_create_at) VALUES(?,?,?) "
                "ON CONFLICT(channel_id) DO UPDATE SET last_post_id=excluded.last_post_id,last_post_create_at=excluded.last_post_create_at "
                "WHERE excluded.last_post_create_at > channel_state.last_post_create_at "
                "OR (excluded.last_post_create_at = channel_state.last_post_create_at AND excluded.last_post_id > channel_state.last_post_id)",
                (channel_id, post_id, timestamp),
            )
