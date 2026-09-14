from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from mattermostdriver.exceptions import NoAccessTokenProvided

from mattermost_adapter.config import AdapterConfig
from mattermost_adapter.errors import AuthError, ConfigError
from mattermost_adapter.models import OutboundMessage
from mattermost_adapter.transport import (INITIAL_RECONNECT_DELAY, MattermostTransport,
                                          SingleConnectionWebsocket, is_permanent_auth_error,
                                          run_with_reconnect)


def options(**extra):
    value = {"debug": False, "verify": False, "scheme": "http", "url": "localhost",
             "port": 8065, "basepath": "/api/v4", "websocket_kw_args": None,
             "timeout": 30, "keepalive": False, "websocket_recv_gap_threshold": 60,
             "event_loop_lag_threshold": 1, "event_loop_watchdog_heartbeat_interval": 300}
    value.update(extra)
    return value


def test_config_validates_token_numeric_values_and_queue_before_network():
    with pytest.raises(ConfigError, match="MM_TOKEN"):
        AdapterConfig.from_env({})
    with pytest.raises(ConfigError, match="MM_REQUEST_TIMEOUT_SECONDS"):
        AdapterConfig.from_env({"MM_TOKEN": "x", "MM_REQUEST_TIMEOUT_SECONDS": "nan"})
    with pytest.raises(ConfigError, match="MM_BRIDGE_QUEUE_CAPACITY"):
        AdapterConfig.from_env({"MM_TOKEN": "x", "MM_BRIDGE_QUEUE_CAPACITY": "100"})
    config = AdapterConfig.from_env({"MM_TOKEN": "  token  ", "MM_REQUEST_TIMEOUT_SECONDS": "12.5",
                                     "MM_EVENT_LOOP_WATCHDOG_HEARTBEAT_SECONDS": "45.5",
                                     "MM_BRIDGE_QUEUE_CAPACITY": "7"})
    assert config.token == "token" and config.request_timeout == 12.5
    assert config.event_loop_watchdog_heartbeat == 45.5
    assert config.queue_capacity == 7 < 100
    assert "token='***'" in repr(config) and "token  " not in repr(config)


def test_create_post_body_preserves_props_and_configurable_reply_key(capsys):
    class Posts:
        def __init__(self): self.body = None
        def create_post(self, body):
            self.body = body
            return {"id": "sent", "channel_id": "c", "root_id": "root", "create_at": 10}
    driver = SimpleNamespace(posts=Posts(), client=SimpleNamespace(userid="bot", username="robot"))
    transport = MattermostTransport(driver, reply_prop_key="mindalert_cevap_post_id")
    sent = transport.send_message(OutboundMessage("c", "answer", "root", "reply", {"trace": "x"}))
    assert driver.posts.body == {"channel_id": "c", "message": "answer", "root_id": "root",
                                 "props": {"trace": "x", "mindalert_cevap_post_id": "reply"}}
    assert sent.thread_id == "root"
    log_line = capsys.readouterr().err
    assert '"event":"Mattermost REST create_post cagrisi tamamlandi"' in log_line
    assert '"channel_id":"c"' in log_line and '"thread_id":"root"' in log_line
    assert '"message_id":"sent"' in log_line
    assert '"reply_to_message_id":"reply"' in log_line and '"duration_ms":' in log_line


def test_watchdog_threshold_and_receive_gap_emit_structured_events(capsys, monkeypatch):
    websocket = SingleConnectionWebsocket(options(websocket_recv_gap_threshold=.5,
                                                   event_loop_lag_threshold=.1), "token")
    times = iter([1.0, 1.1, 2.0])
    import mattermost_adapter.transport as module
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: next(times)))

    async def handler(_raw): pass
    measured = websocket._recv_gap_measuring_handler(handler)

    async def scenario():
        await measured('{"event":"hello"}')
        await measured('{"event":"status_change"}')
        await measured('{"event":"posted"}')
        clocks = iter([1.0, 2.25])
        calls = 0
        async def stop_after_two(_seconds):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise asyncio.CancelledError
        with pytest.raises(asyncio.CancelledError):
            await websocket._event_loop_lag_watchdog(sleep=stop_after_two, clock=lambda: next(clocks))

    asyncio.run(scenario())
    stderr = capsys.readouterr().err
    assert '"event":"websocket recv() boslugu"' in stderr
    assert '"duration_ms":900' in stderr
    assert '"event":"olay dongusu lag esigi asildi"' in stderr
    assert '"lag_ms":250' in stderr


def test_watchdog_heartbeat_reports_window_maximum(capsys):
    websocket = SingleConnectionWebsocket(options(event_loop_lag_threshold=10,
        event_loop_watchdog_heartbeat_interval=3), "token")
    clocks = iter([10.0, 11.05, 12.4, 13.4, 14.6, 15.6, 16.6])
    calls = 0

    async def stop_on_seventh(_seconds):
        nonlocal calls
        calls += 1
        if calls == 7:
            raise asyncio.CancelledError

    async def scenario():
        with pytest.raises(asyncio.CancelledError):
            await websocket._event_loop_lag_watchdog(sleep=stop_on_seventh,
                                                      clock=lambda: next(clocks))

    asyncio.run(scenario())
    lines = [line for line in capsys.readouterr().err.splitlines() if "watchdog canli" in line]
    assert len(lines) == 2
    assert '"maximum_lag_ms":350' in lines[0]
    assert '"maximum_lag_ms":200' in lines[1]


def test_websocket_close_cancels_and_collects_watchdog(monkeypatch):
    import mattermost_adapter.transport as module

    class Socket:
        closed = False

        async def close(self):
            self.closed = True

    socket = Socket()

    async def connect(*_args, **_kwargs):
        return socket

    monkeypatch.setattr(module.websockets, "connect", connect)
    cleaned = False

    class TestWebsocket(SingleConnectionWebsocket):
        async def _authenticate_websocket(self, _socket, _handler):
            await asyncio.sleep(0)

        async def _start_loop(self, _socket, _handler):
            return

        async def _event_loop_lag_watchdog(self, **_kwargs):
            nonlocal cleaned
            try:
                await asyncio.Future()
            finally:
                cleaned = True

    websocket = TestWebsocket(options(), "token")

    async def handler(_raw):
        return None

    asyncio.run(websocket.connect(handler))
    assert cleaned and socket.closed


def test_permanent_auth_classification_and_reconnect_after_clean_close(monkeypatch):
    assert is_permanent_auth_error(NoAccessTokenProvided("rejected"))
    import mattermost_adapter.transport as module
    drivers = [SimpleNamespace(), SimpleNamespace()]
    built = []
    sleeps = []

    def build(_config):
        result = drivers[len(built)]
        result.client = SimpleNamespace(userid="bot", username="robot")
        built.append(result)
        return result

    class StopRun(BaseException): pass
    class FakeListener:
        websocket_connected = False
        def __init__(self, sequence): self.sequence = sequence
        def run_forever(self, websocket_cls=None):
            if self.sequence == 1: return
            raise StopRun

    monkeypatch.setattr(module, "build_driver", build)
    created = []
    def factory(_transport):
        result = FakeListener(len(created) + 1)
        created.append(result)
        return result
    with pytest.raises(StopRun):
        run_with_reconnect(AdapterConfig(token="x"), factory, sleep=sleeps.append)
    assert len(created) == 2 and sleeps == [INITIAL_RECONNECT_DELAY]


def test_permanent_login_error_does_not_sleep(monkeypatch):
    import mattermost_adapter.transport as module
    monkeypatch.setattr(module, "build_driver", lambda _config: (_ for _ in ()).throw(NoAccessTokenProvided("no")))
    with pytest.raises(AuthError):
        run_with_reconnect(AdapterConfig(token="x"), lambda _transport: None,
                           sleep=lambda _seconds: pytest.fail("must not sleep"))
