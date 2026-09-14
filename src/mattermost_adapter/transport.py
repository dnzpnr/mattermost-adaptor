from __future__ import annotations

import asyncio
import json
import logging as std_logging
import os
import ssl
import threading
import time
from pathlib import Path
from typing import Any, Callable

import requests
import websockets
from mattermostdriver import Driver
from mattermostdriver.exceptions import NoAccessTokenProvided, NotEnoughPermissions
from mattermostdriver.websocket import Websocket

from .config import AdapterConfig
from .errors import AuthError, MattermostAPIError
from .logging import log
from .models import FileInfo, OutboundMessage, SentMessage


INITIAL_RECONNECT_DELAY = 1.0
MAX_RECONNECT_DELAY = 30.0


def is_permanent_auth_error(exc: BaseException) -> bool:
    if isinstance(exc, (AuthError, NoAccessTokenProvided, NotEnoughPermissions)):
        return True
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None) in {401, 403}


def _safe_error(exc: BaseException) -> str:
    if is_permanent_auth_error(exc):
        return "Mattermost authentication or permission was rejected"
    return f"{type(exc).__name__}: Mattermost request failed"


def translate_mattermost_error(exc: BaseException) -> AdapterException:
    if isinstance(exc, (AuthError, MattermostAPIError)):
        return exc
    if is_permanent_auth_error(exc):
        return AuthError(_safe_error(exc))
    if isinstance(exc, (requests.RequestException, OSError, ConnectionError, TimeoutError)):
        return MattermostAPIError(_safe_error(exc))
    return MattermostAPIError(_safe_error(exc))


# A narrow type alias keeps the helper's public annotation useful without
# forcing callers to know the third-party exception hierarchy.
AdapterException = AuthError | MattermostAPIError


def build_driver(config: AdapterConfig) -> Driver:
    # The dependency logs server-provided error strings verbatim. Those strings
    # can reflect Authorization, so adapter-owned structured logs are the only
    # output channel enabled for it.
    std_logging.getLogger("mattermostdriver.websocket").setLevel(std_logging.CRITICAL + 1)
    driver = Driver({
        "url": config.url,
        "port": config.port,
        "scheme": config.scheme,
        "token": config.token,
        "request_timeout": config.request_timeout,
        "websocket_recv_gap_threshold": config.websocket_recv_gap_threshold,
        "event_loop_lag_threshold": config.event_loop_lag_threshold,
        "event_loop_watchdog_heartbeat_interval": config.event_loop_watchdog_heartbeat,
        "keepalive": False,
        "debug": False,
    })
    try:
        driver.login()
    except Exception as exc:
        raise translate_mattermost_error(exc) from exc
    return driver


class SingleConnectionWebsocket(Websocket):
    WATCHDOG_INTERVAL_SECONDS = 1.0

    def __init__(self, options: dict[str, Any], token: str):
        super().__init__(options, token)
        self._recv_gap_threshold = float(options.get("websocket_recv_gap_threshold", 60.0))
        self._event_loop_lag_threshold = float(options.get("event_loop_lag_threshold", 1.0))
        self._watchdog_heartbeat_interval = float(options.get("event_loop_watchdog_heartbeat_interval", 300.0))
        self._socket = None

    @staticmethod
    def _event_name(raw_message: str) -> str:
        try:
            return str(json.loads(raw_message).get("event") or "<unnamed>")
        except (AttributeError, TypeError, ValueError):
            return "<unparsed>"

    def _recv_gap_measuring_handler(self, event_handler):
        last_event_at: float | None = None
        last_event_name = "<none>"

        async def measured_handler(raw_message: str) -> None:
            nonlocal last_event_at, last_event_name
            now = time.monotonic()
            event_name = self._event_name(raw_message)
            if last_event_at is not None:
                gap = now - last_event_at
                if gap > self._recv_gap_threshold:
                    log("websocket recv() boslugu", level="WARNING", duration_ms=round(gap * 1000),
                        threshold_ms=round(self._recv_gap_threshold * 1000),
                        previous_event=last_event_name, websocket_event=event_name)
            last_event_at = now
            last_event_name = event_name
            await event_handler(raw_message)

        return measured_handler

    async def _event_loop_lag_watchdog(self, *, sleep=asyncio.sleep, clock=None):
        if clock is None:
            clock = asyncio.get_running_loop().time
        started = clock()
        expected = started + self.WATCHDOG_INTERVAL_SECONDS
        heartbeat_at = started + self._watchdog_heartbeat_interval
        maximum_lag = 0.0
        while True:
            await sleep(self.WATCHDOG_INTERVAL_SECONDS)
            now = clock()
            lag = max(0.0, now - expected)
            maximum_lag = max(maximum_lag, lag)
            if lag > self._event_loop_lag_threshold:
                log("olay dongusu lag esigi asildi", level="WARNING",
                    lag_ms=round(lag * 1000), threshold_ms=round(self._event_loop_lag_threshold * 1000))
            if now >= heartbeat_at:
                log("watchdog canli", window_seconds=self._watchdog_heartbeat_interval,
                    maximum_lag_ms=round(maximum_lag * 1000))
                maximum_lag = 0.0
                heartbeat_at = now + self._watchdog_heartbeat_interval
            expected = now + self.WATCHDOG_INTERVAL_SECONDS

    async def _start_loop(self, websocket, event_handler):
        """Receive with a heartbeat timeout without cancelling the handler.

        Once ``recv`` returns, the event belongs to the adapter. Backpressure
        in ``event_handler`` must therefore be allowed to take as long as the
        queue requires instead of being cancelled by the receive timeout.
        """
        while self._alive:
            try:
                raw_message = await asyncio.wait_for(
                    websocket.recv(), timeout=self.options["timeout"]
                )
            except asyncio.TimeoutError:
                await websocket.pong()
                continue
            await event_handler(raw_message)

    async def connect(self, event_handler):
        context = ssl.create_default_context()
        if not self.options["verify"]:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        scheme = "wss://"
        if self.options["scheme"] != "https":
            scheme = "ws://"
            context = None
        url = "{scheme}{url}:{port}{basepath}/websocket".format(
            scheme=scheme, url=self.options["url"], port=self.options["port"],
            basepath=self.options["basepath"])
        kwargs = self.options.get("websocket_kw_args") or {}
        self._alive = True
        websocket = await websockets.connect(url, ssl=context, **kwargs)
        self._socket = websocket
        measured_handler = self._recv_gap_measuring_handler(event_handler)
        watchdog_task = asyncio.create_task(self._event_loop_lag_watchdog(), name="mattermost-event-loop-lag-watchdog")
        try:
            await self._authenticate_websocket(websocket, measured_handler)
            await self._start_loop(websocket, measured_handler)
        finally:
            watchdog_task.cancel()
            await asyncio.gather(watchdog_task, return_exceptions=True)
            await websocket.close()
            self._socket = None


class MattermostTransport:
    def __init__(self, driver: Driver, *, reply_prop_key: str = "reply_to_post_id") -> None:
        self.driver = driver
        self.reply_prop_key = reply_prop_key

    @property
    def bot_user_id(self) -> str:
        return str(self.driver.client.userid)

    @property
    def username(self) -> str:
        return str(self.driver.client.username)

    def send_message(self, message: OutboundMessage) -> SentMessage:
        props = dict(message.props)
        if message.reply_to_message_id:
            props[self.reply_prop_key] = message.reply_to_message_id
        body = {"channel_id": message.channel_id, "message": message.text,
                "root_id": message.thread_id or "", "props": props}
        started = time.perf_counter()
        result = None
        try:
            result = self.driver.posts.create_post(body)
        except Exception as exc:
            raise translate_mattermost_error(exc) from exc
        finally:
            log("Mattermost REST create_post cagrisi tamamlandi", channel_id=message.channel_id,
                thread_id=message.thread_id,
                message_id=str(result.get("id") or "") if result else None,
                reply_to_message_id=message.reply_to_message_id,
                duration_ms=round((time.perf_counter() - started) * 1000))
        return SentMessage(message_id=str(result.get("id") or ""),
                           channel_id=str(result.get("channel_id") or message.channel_id),
                           thread_id=str(result.get("root_id") or result.get("id") or message.thread_id or "") or None,
                           create_at=result.get("create_at"))

    def get_file_info(self, file_id: str) -> dict[str, Any]:
        try:
            return self.driver.files.get_file_metadata(file_id)
        except Exception as exc:
            raise translate_mattermost_error(exc) from exc

    get_file_metadata = get_file_info

    def get_file_bytes(self, file_id: str) -> bytes:
        try:
            response = self.driver.files.get_file(file_id)
            return bytes(response.content)
        except Exception as exc:
            raise translate_mattermost_error(exc) from exc

    def fetch_file(self, file_id: str, output_dir: str | os.PathLike[str]) -> FileInfo:
        info = self.get_file_info(file_id)
        raw_name = os.path.basename(str(info.get("name") or "")) or file_id
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        stem, suffix = os.path.splitext(raw_name)
        destination = directory / raw_name
        counter = 1
        while destination.exists():
            destination = directory / f"{stem} ({counter}){suffix}"
            counter += 1
        resolved_dir = directory.resolve()
        destination = destination.resolve()
        if destination.parent != resolved_dir:
            raise MattermostAPIError("Refusing to write outside output directory")
        destination.write_bytes(self.get_file_bytes(file_id))
        return FileInfo(file_id=file_id, name=destination.name, size=info.get("size"),
                        mime_type=info.get("mime_type"), path=str(destination))


def run_with_reconnect(config: AdapterConfig, listener_factory: Callable[[MattermostTransport], Any],
                       *, sleep: Callable[[float], None] | None = None,
                       stop_requested: Callable[[], bool] = lambda: False) -> None:
    delay = INITIAL_RECONNECT_DELAY
    attempt = 0
    while not stop_requested():
        attempt += 1
        if attempt > 1:
            log("reconnect attempt", attempt=attempt)
        try:
            driver = build_driver(config)
        except Exception as exc:
            if is_permanent_auth_error(exc):
                raise AuthError("Mattermost authentication or permission was rejected") from exc
            log("Mattermost login/connection error", level="ERROR", error=_safe_error(exc))
            listener = None
        else:
            transport = MattermostTransport(driver, reply_prop_key=config.reply_prop_key)
            listener = listener_factory(transport)
            log("Mattermost login completed", bot_user_id=transport.bot_user_id, username=transport.username)
            try:
                listener.run_forever(websocket_cls=SingleConnectionWebsocket)
            except Exception as exc:
                log("websocket/connection error", level="ERROR", error=_safe_error(exc))
            else:
                log("websocket closed; reconnect scheduled")
        if stop_requested():
            return
        if listener is not None and listener.websocket_connected:
            delay = INITIAL_RECONNECT_DELAY
        log("reconnect scheduled", delay_seconds=delay)
        if sleep is not None:
            # Tests and embedding callers can retain deterministic virtual
            # time by supplying the original injection point.
            sleep(delay)
        else:
            owner = getattr(stop_requested, "__self__", None)
            wait = getattr(owner, "wait", None)
            if isinstance(owner, threading.Event) and callable(wait):
                wait(delay)
            else:
                # Support arbitrary stop predicates while bounding shutdown
                # latency. Event-backed production use takes the exact path
                # above and wakes immediately.
                deadline = time.monotonic() + delay
                waiter = threading.Event()
                while not stop_requested():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    waiter.wait(min(remaining, 0.05))
        delay = min(delay * 2, MAX_RECONNECT_DELAY)
