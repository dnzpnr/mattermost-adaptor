from __future__ import annotations

import asyncio
import json
import ssl
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from mattermost_adapter import cli
from mattermost_adapter.config import AdapterConfig
from mattermost_adapter.listener import MattermostListener
from mattermost_adapter.logging import configure
from mattermost_adapter.models import OutboundMessage
from mattermost_adapter.transport import (
    MattermostTransport,
    SingleConnectionWebsocket,
    run_with_reconnect,
)


def websocket_options(**extra):
    options = {
        "debug": False,
        "verify": False,
        "scheme": "https",
        "url": "localhost",
        "port": 8065,
        "basepath": "/api/v4",
        "websocket_kw_args": None,
        "timeout": 30,
        "keepalive": False,
        "websocket_recv_gap_threshold": 60,
        "event_loop_lag_threshold": 1,
        "event_loop_watchdog_heartbeat_interval": 300,
    }
    options.update(extra)
    return options


def test_1_https_connect_uses_a_client_ssl_context(monkeypatch):
    import mattermost_adapter.transport as module

    captured = {}

    class Socket:
        async def close(self):
            return None

    async def connect(_url, *, ssl, **_kwargs):
        captured["context"] = ssl
        return Socket()

    monkeypatch.setattr(module.websockets, "connect", connect)

    class ProbeWebsocket(SingleConnectionWebsocket):
        async def _authenticate_websocket(self, _socket, _handler):
            return None

        async def _start_loop(self, _socket, _handler):
            return None

        async def _event_loop_lag_watchdog(self, **_kwargs):
            await asyncio.Future()

    async def handler(_message):
        return None

    asyncio.run(ProbeWebsocket(websocket_options(), "token").connect(handler))

    context = captured["context"]
    assert context.check_hostname is False
    assert context.verify_mode == ssl.CERT_NONE
    ssl_object = context.wrap_bio(
        ssl.MemoryBIO(), ssl.MemoryBIO(), server_side=False, server_hostname="localhost"
    )
    assert isinstance(ssl_object, ssl.SSLObject)


def test_2_receive_timeout_does_not_cancel_blocked_handler():
    websocket = SingleConnectionWebsocket(websocket_options(timeout=0.01), "token")
    websocket._alive = True
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()
    handled = []
    cancelled = []

    class Socket:
        def __init__(self):
            self.receive_count = 0
            self.pong_count = 0

        async def recv(self):
            self.receive_count += 1
            if self.receive_count == 1:
                return "already-received-post"
            await asyncio.Future()

        async def pong(self):
            self.pong_count += 1

    socket = Socket()

    async def handler(message):
        handler_started.set()
        try:
            await release_handler.wait()
        except asyncio.CancelledError:
            cancelled.append(message)
            raise
        handled.append(message)

    async def scenario():
        task = asyncio.create_task(websocket._start_loop(socket, handler))
        await handler_started.wait()
        await asyncio.sleep(0.04)
        release_handler.set()
        await asyncio.sleep(0.02)
        websocket._alive = False
        await task

    asyncio.run(scenario())

    assert handled == ["already-received-post"]
    assert cancelled == []


def test_3_help_propagates_clean_system_exit_without_error_envelope(capsys):
    with pytest.raises(SystemExit) as caught:
        cli.main(["--help"])

    captured = capsys.readouterr()
    assert caught.value.code == 0
    assert "usage: mattermost-adapter" in captured.out
    assert "INTERNAL_ERROR" not in captured.out
    assert captured.err == ""


def test_5_reconnect_backoff_wakes_when_stop_event_is_set(monkeypatch):
    import mattermost_adapter.transport as module

    attempted = threading.Event()

    class ObservableStopEvent(threading.Event):
        def __init__(self):
            super().__init__()
            self.wait_started = threading.Event()

        def wait(self, timeout=None):
            self.wait_started.set()
            return super().wait(timeout)

    stop = ObservableStopEvent()

    def unavailable(_config):
        attempted.set()
        raise OSError("offline")

    monkeypatch.setattr(module, "build_driver", unavailable)
    thread = threading.Thread(
        target=run_with_reconnect,
        args=(AdapterConfig(token="token"), lambda _transport: None),
        kwargs={"stop_requested": stop.is_set},
    )
    thread.start()
    try:
        assert attempted.wait(0.5)
        assert stop.wait_started.wait(0.5)
        stop.set()
        thread.join(0.5)
        assert not thread.is_alive()
    finally:
        stop.set()
        thread.join(2)


def test_6_public_classes_have_no_turkish_compatibility_aliases():
    listener_aliases = {
        "oturumu_baslat",
        "intake_durdur",
        "oturumu_drain_et",
        "kacirilan_mesajlari_telafi_et",
        "replay_cursorlarini_baslat",
        "_websocket_baglantisini_calistir",
    }
    websocket_aliases = {
        "_olay_adi",
        "_recv_boslugunu_olcen_handler",
        "_olay_dongusu_lag_watchdog",
    }
    assert listener_aliases.isdisjoint(vars(MattermostListener))
    assert websocket_aliases.isdisjoint(vars(SingleConnectionWebsocket))


def test_7_send_log_distinguishes_created_post_from_reply_target(capsys):
    class Posts:
        def create_post(self, body):
            return {
                "id": "created-post",
                "channel_id": body["channel_id"],
                "root_id": body["root_id"],
                "create_at": 1,
            }

    driver = SimpleNamespace(
        posts=Posts(), client=SimpleNamespace(userid="bot", username="robot")
    )
    MattermostTransport(driver).send_message(
        OutboundMessage("channel", "answer", "root", "reply-target")
    )

    record = json.loads(capsys.readouterr().err)
    assert record["message_id"] == "created-post"
    assert record["reply_to_message_id"] == "reply-target"


def test_8_unexpected_cli_error_logs_redacted_traceback(monkeypatch, capsys):
    secret = "round1-secret-token"
    configure("INFO", secrets=[secret])

    def crash(_args):
        raise RuntimeError(f"failure contains {secret}")

    monkeypatch.setattr(cli, "_run", crash)
    try:
        assert cli.main(["health"]) == 1
        captured = capsys.readouterr()
    finally:
        configure("INFO", secrets=[])

    envelope = json.loads(captured.out)
    record = json.loads(captured.err)
    assert envelope["error"]["code"] == "INTERNAL_ERROR"
    assert record["level"] == "ERROR"
    assert record["event"] == "unexpected CLI error"
    assert "Traceback (most recent call last)" in record["traceback"]
    assert "RuntimeError" in record["traceback"]
    assert secret not in captured.out + captured.err


def test_9_request_stop_always_uses_threadsafe_loop_wakeup():
    callbacks = []

    class Loop:
        def call_soon_threadsafe(self, callback):
            callbacks.append(callback)

    class StopEvent:
        was_set = False

        def set(self):
            self.was_set = True

    listener = MattermostListener.__new__(MattermostListener)
    listener._loop = Loop()
    listener._stop_event = StopEvent()

    listener.request_stop()

    assert callbacks == [listener._stop_event.set]
    assert listener._stop_event.was_set is False
    callbacks[0]()
    assert listener._stop_event.was_set is True


def test_10_readme_records_both_upstream_transport_defects():
    readme = Path(__file__).resolve().parents[2] / "README.md"
    text = readme.read_text(encoding="utf-8")
    assert "MindAlert kaynağındaki" in text
    assert "mattermost_bot/run.py" in text
    assert "TLS istemci context'i" in text
    assert "timeout yalnız `websocket.recv()`" in text
    assert "yalnız bu adapter içinde düzeltildi" in text
