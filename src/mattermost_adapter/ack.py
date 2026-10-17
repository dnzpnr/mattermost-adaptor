from __future__ import annotations

import json
import sys
import threading
from dataclasses import dataclass
from typing import IO, Any

from .state import StateStore


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


@dataclass(frozen=True)
class _PendingPost:
    ack_id: str
    channel_id: str
    post_id: str
    create_at: int


class AckController:
    """Teslim onayı (ack) koordinatörü.

    Ack kipinde durum yazımları anında yapılmaz; teslim edilen her mesaj ack_id
    ile sıraya alınır. Tüketici bir ack_id onayladığında o mesaj ve ondan önce
    sıraya girmiş tüm kayıtlar tek seferde kalıcılaşır (sıralı onay).
    """

    def __init__(self, state_store: StateStore, *, stream: IO[str] | None = None) -> None:
        self._state_store = state_store
        self._stream = stream if stream is not None else sys.stdout
        self._lock = threading.Lock()
        self._out_lock = threading.Lock()
        self._pending: list[_PendingPost] = []
        self._index: dict[str, int] = {}
        self._committed: set[str] = set()
        self._reader: threading.Thread | None = None
        self._closed = threading.Event()

    @staticmethod
    def ack_id_for(post_id: str, create_at: Any) -> str:
        return f"{post_id}:{_as_int(create_at)}"

    def prepare(self, channel_id: str, post_id: str, create_at: Any) -> str:
        ack_id = self.ack_id_for(post_id, create_at)
        self._remember(_PendingPost(ack_id, str(channel_id), str(post_id), _as_int(create_at)))
        return ack_id

    def record(self, channel_id: str, post_id: str, create_at: Any = 0) -> None:
        item = _PendingPost(self.ack_id_for(post_id, create_at), str(channel_id), str(post_id),
                            _as_int(create_at))
        self._remember(item)

    def _remember(self, item: _PendingPost) -> None:
        with self._lock:
            if item.ack_id in self._index or item.ack_id in self._committed:
                return
            self._index[item.ack_id] = len(self._pending)
            self._pending.append(item)

    def commit(self, ack_id: str) -> bool:
        with self._lock:
            index = self._index.get(ack_id)
            if index is None:
                batch: list[_PendingPost] | None = None
            else:
                batch = self._pending[:index + 1]
                self._pending = self._pending[index + 1:]
                self._index = {item.ack_id: position for position, item in enumerate(self._pending)}
                self._committed.update(item.ack_id for item in batch)
        if batch is None:
            self.write_line({"type": "ack_error", "ack_id": ack_id,
                             "error": {"code": "UNKNOWN_ACK", "message": f"unknown ack_id: {ack_id}"}})
            return False
        for item in batch:
            self._state_store.mark_post_processed(item.channel_id, item.post_id, item.create_at)
        self.write_line({"type": "ack", "ack_id": ack_id, "status": "committed"})
        return True

    def write_line(self, document: dict[str, Any]) -> None:
        payload = json.dumps(document, ensure_ascii=False, separators=(",", ":"))
        with self._out_lock:
            self._stream.write(payload + "\n")
            self._stream.flush()

    def start(self) -> None:
        if self._reader is not None:
            return
        self._reader = threading.Thread(target=self._read_stdin, name="mattermost-ack-reader",
                                        daemon=True)
        self._reader.start()

    def stop(self) -> None:
        self._closed.set()
        reader, self._reader = self._reader, None
        if reader is not None:
            reader.join(timeout=1.0)

    def _read_stdin(self) -> None:
        while not self._closed.is_set():
            line = sys.stdin.readline()
            if not line:
                return
            line = line.strip()
            if not line:
                continue
            try:
                document = json.loads(line)
            except (TypeError, ValueError):
                continue
            if not isinstance(document, dict):
                continue
            ack_id = document.get("ack")
            if not isinstance(ack_id, str) or not ack_id:
                continue
            self.commit(ack_id)


class AckGatedStateStore:
    """StateStore sarmalayıcı: yazımları AckController'a erteler.

    Okuma yolları (replay/imleç/tekillik) doğrudan iç depoya gider; yalnız
    `mark_post_processed` onay gelene kadar sıraya alınır. Gerçekten teslim
    edilen mesajlar için `AckController.prepare` handler tarafından zaten
    kaydedildiğinden, listener'ın işaretleme çağrısı aynı ack_id ile dedupe olur.
    """

    def __init__(self, inner: StateStore, controller: AckController) -> None:
        self._inner = inner
        self._controller = controller

    def list_channels(self) -> list[str]:
        return self._inner.list_channels()

    def get_last_post_id(self, channel_id: str) -> str | None:
        return self._inner.get_last_post_id(channel_id)

    def is_post_processed(self, post_id: str) -> bool:
        return self._inner.is_post_processed(post_id)

    def initialize_cursor(self, channel_id: str, post_id: str, create_at: int = 0) -> None:
        self._inner.initialize_cursor(channel_id, post_id, create_at)

    def mark_post_processed(self, channel_id: str, post_id: str, create_at: int = 0) -> None:
        self._controller.record(channel_id, post_id, create_at)
