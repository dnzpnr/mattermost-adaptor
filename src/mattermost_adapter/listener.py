from __future__ import annotations

import asyncio
import inspect
import json
import math
import threading
import time
from typing import Any, Awaitable, Protocol

from .logging import correlation_id, log, new_correlation_id
from .models import ConversationRef, NormalizationResult, NormalizedMessage, Session
from .normalize import normalize_post
from .session import SessionResolver
from .state import StateStore
from .transport import MattermostTransport


class MessageHandler(Protocol):
    def handle_message(self, message: NormalizedMessage, session: Session,
                       correlation_id: str) -> None | Awaitable[None]: ...


class MattermostListener:
    REPLAY_LIMIT = 100
    DEFAULT_QUEUE_CAPACITY = 50
    DEFAULT_DRAIN_DEADLINE_SECONDS = 180.0

    def __init__(self, transport: MattermostTransport, resolver: SessionResolver,
                 handler: MessageHandler, state_store: StateStore | None = None,
                 *, queue_capacity: int = DEFAULT_QUEUE_CAPACITY,
                 drain_deadline_seconds: float = DEFAULT_DRAIN_DEADLINE_SECONDS) -> None:
        if queue_capacity <= 0 or queue_capacity >= self.REPLAY_LIMIT:
            raise ValueError(f"queue_capacity must satisfy 0 < value < {self.REPLAY_LIMIT}")
        if not math.isfinite(drain_deadline_seconds) or drain_deadline_seconds <= 0:
            raise ValueError("drain_deadline_seconds must be positive and finite")
        self.transport = transport
        self.driver = transport.driver
        self.resolver = resolver
        self.handler = handler
        self.state_store = state_store
        self.queue_capacity = queue_capacity
        self.drain_deadline_seconds = drain_deadline_seconds
        self.websocket_connected = False
        self._post_queue: asyncio.Queue[tuple[dict[str, Any], dict[str, Any]]] | None = None
        self._hello_barrier: asyncio.Event | None = None
        self._replay_completed: asyncio.Event | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._intake_open = False
        self._replay_active = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event: asyncio.Event | None = None
        self._loop_thread_id: int | None = None

    async def start_session(self) -> asyncio.Task[None]:
        if self._worker_task is not None:
            raise RuntimeError("Listener worker session is already started")
        self.websocket_connected = False
        self._post_queue = asyncio.Queue(maxsize=self.queue_capacity)
        self._hello_barrier = asyncio.Event()
        self._replay_completed = asyncio.Event()
        self._intake_open = True
        self._worker_task = asyncio.create_task(self._run_worker(), name="mattermost-single-post-worker")
        return self._worker_task

    async def stop_intake(self) -> None:
        self._intake_open = False

    async def drain_session(self) -> None:
        worker, queue = self._worker_task, self._post_queue
        barrier, replay_completed = self._hello_barrier, self._replay_completed
        if worker is None or queue is None or barrier is None or replay_completed is None:
            return
        if not barrier.is_set() and not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            self._clear_session()
            return
        drain_task = asyncio.create_task(self._drain_worker(worker, queue, replay_completed),
                                         name="mattermost-worker-drain")
        try:
            done, _ = await asyncio.wait({drain_task}, timeout=self.drain_deadline_seconds)
            if not done:
                log("worker drain deadline asildi", level="WARNING",
                    deadline_seconds=self.drain_deadline_seconds)
            await asyncio.shield(drain_task)
        except asyncio.CancelledError:
            await asyncio.shield(drain_task)
            raise
        finally:
            self._clear_session()

    async def _drain_worker(self, worker: asyncio.Task[None],
                            queue: asyncio.Queue, replay_completed: asyncio.Event) -> None:
        if worker.done():
            await worker
            return
        replay_wait = asyncio.create_task(replay_completed.wait())
        try:
            done, _ = await asyncio.wait({replay_wait, worker}, return_when=asyncio.FIRST_COMPLETED)
            if worker in done:
                await worker
            await replay_wait
        finally:
            if not replay_wait.done():
                replay_wait.cancel()
                await asyncio.gather(replay_wait, return_exceptions=True)
        queue_empty = asyncio.create_task(queue.join())
        try:
            done, _ = await asyncio.wait({queue_empty, worker}, return_when=asyncio.FIRST_COMPLETED)
            if worker in done:
                await worker
            await queue_empty
        finally:
            if not queue_empty.done():
                queue_empty.cancel()
                await asyncio.gather(queue_empty, return_exceptions=True)
        worker.cancel()
        result = (await asyncio.gather(worker, return_exceptions=True))[0]
        if not isinstance(result, asyncio.CancelledError):
            if isinstance(result, BaseException):
                raise result
            raise AssertionError("Worker returned unexpectedly during drain")

    def _clear_session(self) -> None:
        self._intake_open = False
        self._worker_task = None
        self._post_queue = None
        self._hello_barrier = None
        self._replay_completed = None

    async def _run_worker(self) -> None:
        queue, barrier, replay_completed = self._post_queue, self._hello_barrier, self._replay_completed
        assert queue is not None and barrier is not None and replay_completed is not None
        await barrier.wait()
        try:
            await self.replay_missed_messages()
        finally:
            replay_completed.set()
        while True:
            post, event_data = await queue.get()
            try:
                await self._process_post(post, event_data)
            finally:
                queue.task_done()

    async def handle_event(self, raw_message: str | dict[str, Any]) -> None:
        try:
            event = json.loads(raw_message) if isinstance(raw_message, str) else raw_message
        except (TypeError, ValueError):
            return
        if not isinstance(event, dict):
            return
        if event.get("event") == "hello":
            if not self._intake_open or self._hello_barrier is None:
                raise RuntimeError("Listener worker session was not started before hello")
            self.websocket_connected = True
            log("websocket baglandi; kacirilan mesaj taramasi basliyor")
            self._hello_barrier.set()
            return
        if event.get("event") != "posted":
            return
        data = event.get("data")
        if not isinstance(data, dict):
            return
        post = data.get("post")
        if isinstance(post, str):
            try:
                post = json.loads(post)
            except (TypeError, ValueError):
                return
        if not isinstance(post, dict):
            return
        if not self._intake_open or self._post_queue is None:
            raise RuntimeError("Listener worker session is not running; post was not accepted")
        await self._post_queue.put((post, data))

    async def _run_blocking(self, function, /, *args, **kwargs):
        return await asyncio.to_thread(function, *args, **kwargs)

    async def _resolve(self, ref: ConversationRef) -> Session | None:
        function = self.resolver.resolve
        if inspect.iscoroutinefunction(function):
            return await function(ref)
        return await self._run_blocking(function, ref)

    async def _call_handler(self, message: NormalizedMessage, session: Session, trace_id: str) -> None:
        function = getattr(self.handler, "handle_message", None) or getattr(self.handler, "handle", None)
        if function is None and callable(self.handler):
            function = self.handler
        if function is None:
            raise TypeError("MessageHandler must be callable or define handle_message()")
        if inspect.iscoroutinefunction(function):
            await function(message, session, trace_id)
        else:
            result = await self._run_blocking(function, message, session, trace_id)
            if inspect.isawaitable(result):
                await result

    async def _mark_processed(self, post: dict[str, Any]) -> None:
        if self.state_store is None:
            return
        started = time.perf_counter()
        await self._run_blocking(self.state_store.mark_post_processed,
                                 str(post["channel_id"]), str(post["id"]), int(post.get("create_at") or 0))
        log("SQLite post yazimi tamamlandi", channel_id=str(post["channel_id"]),
            message_id=str(post["id"]), duration_ms=round((time.perf_counter() - started) * 1000))

    async def _process_post(self, post: dict[str, Any], event_data: dict[str, Any] | None = None) -> bool:
        started = time.perf_counter()
        post_id = str(post.get("id") or "")
        channel_id = str(post.get("channel_id") or "")
        thread_id = str(post.get("root_id") or post_id or "")
        trace_id = new_correlation_id()
        token = correlation_id.set(trace_id)
        branch = "validation"
        session: Session | None = None
        log("post alindi", channel_id=channel_id or None, thread_id=thread_id or None,
            message_id=post_id or None, session_id=None, correlation_id=trace_id,
            create_at=post.get("create_at") or 0)
        try:
            if not post_id or not channel_id:
                branch = "invalid_post"
                return False
            if self.state_store is not None and await self._run_blocking(self.state_store.is_post_processed, post_id):
                branch = "already_processed"
                return False
            if post.get("user_id") == self.transport.bot_user_id:
                branch = "own_message_ignored"
                log("post isleme basladi", channel_id=channel_id, thread_id=thread_id,
                    message_id=post_id, session_id=None, branch=branch)
                await self._mark_processed(post)
                return False
            if str(post.get("type") or "").startswith("system_"):
                branch = "system_message_ignored"
                log("post isleme basladi", channel_id=channel_id, thread_id=thread_id,
                    message_id=post_id, session_id=None, branch=branch)
                await self._mark_processed(post)
                return False
            message = normalize_post(post, event_data)
            ref = ConversationRef(provider="mattermost", channel_id=message.channel_id, thread_id=message.thread_id)
            session = await self._resolve(ref)
            if session is None:
                branch = "unmapped_conversation"
                log("post isleme basladi", channel_id=channel_id, thread_id=thread_id,
                    message_id=post_id, session_id=None, branch=branch)
                return False
            if not message.attachments and not message.content.strip():
                branch = "empty_message"
                log("post isleme basladi", channel_id=channel_id, thread_id=thread_id,
                    message_id=post_id, session_id=session.session_id, branch=branch)
                await self._mark_processed(post)
                return False
            branch = "handle_message"
            log("post isleme basladi", channel_id=channel_id, thread_id=thread_id,
                message_id=post_id, session_id=session.session_id, branch=branch)
            await self._call_handler(message, session, trace_id)
            await self._mark_processed(post)
            return True
        finally:
            log("post isleme bitti", channel_id=channel_id or None, thread_id=thread_id or None,
                message_id=post_id or None, session_id=session.session_id if session else None,
                branch=branch, duration_ms=round((time.perf_counter() - started) * 1000))
            correlation_id.reset(token)

    @staticmethod
    def _sort_posts(response: dict[str, Any]) -> list[dict[str, Any]]:
        posts = response.get("posts") or {}
        selected = [posts[post_id] for post_id in response.get("order") or [] if post_id in posts]
        return sorted(selected, key=lambda post: (int(post.get("create_at") or 0), str(post.get("id") or "")))

    def _get_replay_posts(self, channel_id: str, *, params: dict[str, Any]) -> dict[str, Any]:
        started = time.perf_counter()
        try:
            return self.driver.posts.get_posts_for_channel(channel_id, params=params)
        finally:
            log("Mattermost REST get_posts_for_channel cagrisi tamamlandi", channel_id=channel_id,
                duration_ms=round((time.perf_counter() - started) * 1000))

    async def replay_missed_messages(self) -> int:
        started = time.perf_counter()
        total = 0
        try:
            if self.state_store is None:
                log("replay durumu yapilandirilmamis; telafi atlandi", level="WARNING")
                return 0
            channels = await self._run_blocking(self.state_store.list_channels)
            for channel_id in channels:
                last_post_id = await self._run_blocking(self.state_store.get_last_post_id, channel_id)
                if last_post_id is None:
                    response = await self._run_blocking(self._get_replay_posts, channel_id,
                                                        params={"page": 0, "per_page": 1})
                    newest = self._sort_posts(response)
                    if newest:
                        await self._mark_processed(newest[-1])
                        log("replay cursor'u baslatildi", channel_id=channel_id,
                            message_id=str(newest[-1]["id"]))
                    continue
                params: dict[str, Any] = {"page": 0, "per_page": self.REPLAY_LIMIT + 1}
                if last_post_id:
                    params["after"] = last_post_id
                response = await self._run_blocking(self._get_replay_posts, channel_id, params=params)
                posts = self._sort_posts(response)
                overflow = len(posts) > self.REPLAY_LIMIT or bool(response.get("next_post_id"))
                if overflow:
                    log("replay ust siniri asildi", level="WARNING", channel_id=channel_id,
                        replay_limit=self.REPLAY_LIMIT)
                    response = await self._run_blocking(self._get_replay_posts, channel_id,
                                                        params={"page": 0, "per_page": self.REPLAY_LIMIT})
                    posts = self._sort_posts(response)
                channel_total = 0
                self._replay_active = True
                try:
                    for post in posts[-self.REPLAY_LIMIT:]:
                        if await self._process_post(post, {}):
                            channel_total += 1
                finally:
                    self._replay_active = False
                total += channel_total
                if channel_total:
                    log("replay tamamlandi", channel_id=channel_id, replayed=channel_total)
            return total
        finally:
            log("kacirilan mesaj taramasi tamamlandi", replayed=total,
                duration_ms=round((time.perf_counter() - started) * 1000))

    def initialize_replay_cursors(self) -> None:
        if self.state_store is None:
            return
        for channel_id in self.state_store.list_channels():
            if self.state_store.get_last_post_id(channel_id) is not None:
                continue
            response = self.driver.posts.get_posts_for_channel(channel_id, params={"page": 0, "per_page": 1})
            newest = self._sort_posts(response)
            post = newest[-1] if newest else None
            self.state_store.initialize_cursor(channel_id, str(post["id"]) if post else "",
                                               int(post.get("create_at") or 0) if post else 0)
            log("replay cursor'u websocket oncesi baslatildi", channel_id=channel_id,
                message_id=str(post["id"]) if post else None)

    async def _run_websocket_connection(self, websocket, external_stop=None) -> None:
        worker = await self.start_session()
        receiver = asyncio.create_task(websocket.connect(self.handle_event), name="mattermost-websocket-receiver")
        stopper = asyncio.create_task(external_stop.wait()) if external_stop is not None else None
        watched = {receiver, worker}
        if stopper is not None:
            watched.add(stopper)
        try:
            done, _ = await asyncio.wait(watched, return_when=asyncio.FIRST_COMPLETED)
            if worker in done:
                await self.stop_intake()
                receiver.cancel()
                await asyncio.gather(receiver, return_exceptions=True)
                await self.drain_session()
                raise AssertionError("Worker returned unexpectedly without an error")
            if stopper is not None and stopper in done:
                await self.stop_intake()
                receiver.cancel()
                await asyncio.gather(receiver, return_exceptions=True)
                await self.drain_session()
                return
            await self.stop_intake()
            await self.drain_session()
            await receiver
        finally:
            await self.stop_intake()
            if self._worker_task is not None:
                await self.drain_session()
            if not receiver.done():
                receiver.cancel()
                await asyncio.gather(receiver, return_exceptions=True)
            if stopper is not None and not stopper.done():
                stopper.cancel()
                await asyncio.gather(stopper, return_exceptions=True)

    def request_stop(self) -> None:
        if self._loop is not None and self._stop_event is not None:
            if threading.get_ident() == self._loop_thread_id:
                self._stop_event.set()
            else:
                self._loop.call_soon_threadsafe(self._stop_event.set)

    def run_forever(self, websocket_cls=None) -> None:
        self.initialize_replay_cursors()
        if websocket_cls is None:
            from mattermostdriver.websocket import Websocket
            websocket_cls = Websocket
        self.driver.websocket = websocket_cls(self.driver.options, self.driver.client.token)
        loop = asyncio.new_event_loop()
        self._loop = loop
        self._loop_thread_id = threading.get_ident()
        self._stop_event = asyncio.Event()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._run_websocket_connection(self.driver.websocket, self._stop_event))
        finally:
            self._loop = None
            self._loop_thread_id = None
            self._stop_event = None
            loop.close()
            asyncio.set_event_loop(None)

    # Source-name aliases are intentionally tiny and aid behavior-port review.
    oturumu_baslat = start_session
    intake_durdur = stop_intake
    oturumu_drain_et = drain_session
    kacirilan_mesajlari_telafi_et = replay_missed_messages
    replay_cursorlarini_baslat = initialize_replay_cursors
    _websocket_baglantisini_calistir = _run_websocket_connection
