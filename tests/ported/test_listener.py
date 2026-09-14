from __future__ import annotations

import asyncio
import json
import threading

import pytest

from mattermost_adapter.listener import MattermostListener
from mattermost_adapter.session import ThreadSessionResolver
from mattermost_adapter.state import SqliteStateStore
from mattermost_adapter.transport import MattermostTransport


def post(post_id, create_at, *, message="hello", user_id="user", channel_id="c1", root_id="", post_type=""):
    return {"id": post_id, "create_at": create_at, "channel_id": channel_id,
            "user_id": user_id, "message": message, "root_id": root_id, "type": post_type,
            "props": {}}


def event(item):
    return json.dumps({"event": "posted", "data": {"post": json.dumps(item)}})


class FakePosts:
    def __init__(self, channel_posts=None):
        self.channel_posts = channel_posts or {}
        self.get_calls = []
        self.created = []

    def create_post(self, body):
        self.created.append(body)
        return {"id": "sent", "channel_id": body["channel_id"], "root_id": body["root_id"], "create_at": 3}

    def get_posts_for_channel(self, channel_id, params=None):
        params = dict(params or {})
        self.get_calls.append((channel_id, params))
        items = list(self.channel_posts.get(channel_id, []))
        after = params.get("after")
        if after:
            ids = [item["id"] for item in items]
            items = items[ids.index(after) + 1:] if after in ids else []
        count = int(params.get("per_page", 60))
        selected = items[-count:]
        return {"order": [item["id"] for item in reversed(selected)],
                "posts": {item["id"]: item for item in selected},
                "next_post_id": "more" if len(items) > count else ""}


class FakeDriver:
    def __init__(self, posts=None):
        self.posts = posts or FakePosts()
        self.client = type("Client", (), {"userid": "bot", "username": "robot", "token": "token"})()
        self.options = {}


class RecordingHandler:
    def __init__(self):
        self.calls = []

    def handle_message(self, message, session, trace_id):
        self.calls.append((message.message_id, message.content, session.session_id, trace_id))


def listener(tmp_path, *, items=None, handler=None, state=True, queue_capacity=50, resolver=None):
    posts = FakePosts({"c1": items or []})
    transport = MattermostTransport(FakeDriver(posts))
    store = SqliteStateStore(tmp_path / "state.db", ["c1"]) if state else None
    handler = handler or RecordingHandler()
    result = MattermostListener(transport, resolver or ThreadSessionResolver(), handler, store,
                                queue_capacity=queue_capacity)
    return result, posts, store, handler


async def run_session(instance, *events):
    await instance.start_session()
    event_names = []
    for item in events:
        try:
            event_names.append(json.loads(item).get("event"))
        except (AttributeError, ValueError):
            event_names.append(None)
    if "hello" not in event_names:
        await instance.handle_event('{"event":"hello","seq":0}')
    for item in events:
        await instance.handle_event(item)
    await instance.stop_intake()
    await instance.drain_session()


def run(coroutine):
    loop = asyncio.new_event_loop()

    async def with_test_heartbeat():
        # The command sandbox cannot wake an event loop through its self-pipe;
        # a test-only timer lets asyncio deliver completed to_thread futures.
        task = asyncio.create_task(coroutine)
        while not task.done():
            await asyncio.sleep(.01)
        return await task

    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(with_test_heartbeat())
    finally:
        loop.close()
        asyncio.set_event_loop(None)


def test_filters_and_dedupe_preserve_cursor_semantics(tmp_path):
    instance, _, store, handler = listener(tmp_path)
    run(run_session(instance,
        event(post("own", 1, user_id="bot")),
        event(post("system", 2, post_type="system_join_channel")),
        event(post("empty", 3, message="  ")),
        event(post("ok", 4, message="yes")),
        event(post("ok", 4, message="yes"))))
    assert [call[0] for call in handler.calls] == ["ok"]
    assert all(store.is_post_processed(item) for item in ["own", "system", "empty", "ok"])
    assert store.get_last_post_id("c1") == "ok"


def test_unmapped_conversation_is_silent_and_not_processed(tmp_path):
    class Reject:
        def resolve(self, _ref):
            return None
    instance, _, store, handler = listener(tmp_path, resolver=Reject())
    run(run_session(instance, event(post("unmapped", 1))))
    assert handler.calls == []
    assert not store.is_post_processed("unmapped")


def test_replay_after_cursor_is_chronological_and_deduplicated(tmp_path):
    items = [post("p0", 1), post("p1", 2, message="one"), post("p2", 3, message="two")]
    instance, posts, store, handler = listener(tmp_path, items=items)
    store.mark_post_processed("c1", "p0", 1)
    run(run_session(instance, '{"event":"hello","seq":0}', event(items[1])))
    assert [(call[0], call[1]) for call in handler.calls] == [("p1", "one"), ("p2", "two")]
    assert posts.get_calls[0][1]["after"] == "p0"
    assert store.get_last_post_id("c1") == "p2"


def test_replay_skips_invalid_create_at_and_keeps_worker_alive(tmp_path, capsys):
    items = [
        post("bad-time", "not-a-number", message="invalid"),
        post("valid-time", 2, message="still delivered"),
    ]
    instance, _, store, handler = listener(tmp_path, items=items)
    store.initialize_cursor("c1", "", 0)

    async def scenario():
        worker = await instance.start_session()
        await instance.handle_event('{"event":"hello","seq":0}')
        assert instance._replay_completed is not None
        await instance._replay_completed.wait()
        assert not worker.done(), repr(worker.exception() if worker.done() else None)
        await instance.stop_intake()
        await instance.drain_session()

    run(scenario())

    assert [call[0] for call in handler.calls] == ["valid-time"]
    assert store.is_post_processed("valid-time")
    assert not store.is_post_processed("bad-time")
    stderr = capsys.readouterr().err
    assert '"level":"WARNING"' in stderr
    assert '"event":"invalid post skipped"' in stderr
    assert '"message_id":"bad-time"' in stderr


def test_cursor_is_initialized_before_websocket_and_gap_is_replayed(tmp_path):
    instance, posts, store, handler = listener(tmp_path)
    instance.initialize_replay_cursors()
    assert store.get_last_post_id("c1") == ""
    posts.channel_posts["c1"] = [post("between", 2, message="do not miss")]
    run(run_session(instance, '{"event":"hello","seq":0}'))
    assert [call[0] for call in handler.calls] == ["between"]


def test_replay_overflow_processes_newest_one_hundred(tmp_path, capsys):
    items = [post("p0", 0)] + [post(f"p{i}", i, message=str(i)) for i in range(1, 102)]
    instance, _, store, handler = listener(tmp_path, items=items)
    store.mark_post_processed("c1", "p0", 0)
    assert run(instance.replay_missed_messages()) == 100
    assert [call[0] for call in handler.calls] == [f"p{i}" for i in range(2, 102)]
    assert '"event":"replay ust siniri asildi"' in capsys.readouterr().err


def test_hello_barrier_orders_replay_before_queued_live_posts(tmp_path):
    items = [post("p0", 1), post("replay", 2, message="replay")]
    instance, _, store, handler = listener(tmp_path, items=items)
    store.mark_post_processed("c1", "p0", 1)

    async def scenario():
        await instance.start_session()
        await instance.handle_event(event(post("early", 3, message="early")))
        await instance.handle_event('{"event":"hello","seq":0}')
        await instance.stop_intake()
        await instance.drain_session()

    run(scenario())
    assert [call[1] for call in handler.calls] == ["replay", "early"]


def test_single_worker_keeps_order_and_handler_failure_is_original(tmp_path):
    original = RuntimeError("original worker failure")

    class Failing:
        def handle_message(self, *_args):
            raise original

    instance, _, _, _ = listener(tmp_path, handler=Failing(), state=False)

    class WaitingWebsocket:
        async def connect(self, callback):
            await callback('{"event":"hello","seq":0}')
            await callback(event(post("fail", 1)))
            await asyncio.Future()

    async def scenario():
        with pytest.raises(RuntimeError) as caught:
            await instance._run_websocket_connection(WaitingWebsocket())
        assert caught.value is original

    run(scenario())


def test_full_queue_applies_backpressure_without_dropping(tmp_path):
    entered = threading.Event()
    released = threading.Event()

    class Blocking(RecordingHandler):
        def handle_message(self, message, session, trace_id):
            if not self.calls:
                entered.set()
                assert released.wait(2)
            super().handle_message(message, session, trace_id)

    handler = Blocking()
    instance, _, _, _ = listener(tmp_path, handler=handler, state=False, queue_capacity=1)

    async def wait_thread_event(flag):
        while not flag.is_set():
            await asyncio.sleep(0.005)

    async def scenario():
        await instance.start_session()
        await instance.handle_event('{"event":"hello","seq":0}')
        await instance.handle_event(event(post("p1", 1)))
        await wait_thread_event(entered)
        await instance.handle_event(event(post("p2", 2)))
        third = asyncio.create_task(instance.handle_event(event(post("p3", 3))))
        await asyncio.sleep(0)
        assert not third.done()
        released.set()
        await third
        await instance.stop_intake()
        await instance.drain_session()

    run(scenario())
    assert [call[0] for call in handler.calls] == ["p1", "p2", "p3"]


def test_processed_mark_is_after_successful_handler_side_effect(tmp_path):
    order = []

    class Ordered:
        def handle_message(self, *_args):
            order.append("effect")

    instance, _, store, _ = listener(tmp_path, handler=Ordered())
    original = store.mark_post_processed

    def marked(*args):
        if args[1] == "ordered":
            order.append("mark")
        original(*args)

    store.mark_post_processed = marked
    run(run_session(instance, event(post("ordered", 1))))
    assert order == ["effect", "mark"]


def test_slow_handler_does_not_block_receiver_from_accepting_next_post(tmp_path):
    entered = threading.Event()
    release = threading.Event()
    second_received = threading.Event()

    class Slow(RecordingHandler):
        def handle_message(self, message, session, trace_id):
            if not self.calls:
                entered.set()
                assert release.wait(2)
            super().handle_message(message, session, trace_id)

    instance, _, _, handler = listener(tmp_path, handler=Slow(), state=False)

    class SocketClosed(RuntimeError): pass
    class FakeWebsocket:
        async def connect(self, callback):
            await callback('{"event":"hello","seq":0}')
            await callback(event(post("first", 1)))
            while not entered.is_set():
                await asyncio.sleep(.005)
            await callback(event(post("second", 2)))
            second_received.set()
            raise SocketClosed("closed")

    async def scenario():
        task = asyncio.create_task(instance._run_websocket_connection(FakeWebsocket()))
        while not second_received.is_set():
            await asyncio.sleep(.005)
        assert not task.done()
        release.set()
        with pytest.raises(SocketClosed):
            await task

    run(scenario())
    assert [call[0] for call in handler.calls] == ["first", "second"]


def test_disconnect_waits_for_inflight_handler_before_next_session(tmp_path):
    entered = threading.Event()
    release = threading.Event()
    active = 0
    maximum_active = 0

    class Counting(RecordingHandler):
        def handle_message(self, message, session, trace_id):
            nonlocal active, maximum_active
            active += 1
            maximum_active = max(maximum_active, active)
            try:
                if not self.calls:
                    entered.set()
                    assert release.wait(2)
                super().handle_message(message, session, trace_id)
            finally:
                active -= 1

    handler = Counting()
    instance, _, _, _ = listener(tmp_path, handler=handler, state=False)

    class Closed(RuntimeError): pass
    class ClosingWebsocket:
        def __init__(self, post_id): self.post_id = post_id
        async def connect(self, callback):
            await callback('{"event":"hello","seq":0}')
            await callback(event(post(self.post_id, 1)))
            if self.post_id == "first":
                while not entered.is_set():
                    await asyncio.sleep(.005)
            raise Closed(self.post_id)

    async def scenario():
        first = asyncio.create_task(instance._run_websocket_connection(ClosingWebsocket("first")))
        while not entered.is_set():
            await asyncio.sleep(.005)
        await asyncio.sleep(0)
        assert not first.done()
        release.set()
        with pytest.raises(Closed, match="first"):
            await first
        with pytest.raises(Closed, match="second"):
            await instance._run_websocket_connection(ClosingWebsocket("second"))

    run(scenario())
    assert maximum_active == 1
    assert [call[0] for call in handler.calls] == ["first", "second"]


def test_invalid_websocket_json_is_silently_ignored(tmp_path):
    instance, _, _, handler = listener(tmp_path, state=False)
    run(run_session(instance, "not json"))
    assert handler.calls == []


def test_invalid_normalized_post_does_not_kill_worker_or_advance_state(tmp_path, capsys):
    instance, _, store, handler = listener(tmp_path)
    invalid = post("invalid-time", "not-a-number")
    valid = post("valid-after-invalid", 2, message="still delivered")

    run(run_session(instance, event(invalid), event(valid)))

    assert [call[0] for call in handler.calls] == ["valid-after-invalid"]
    assert not store.is_post_processed("invalid-time")
    assert store.is_post_processed("valid-after-invalid")
    records = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    warning = next(record for record in records if record["event"] == "invalid post skipped")
    assert warning["level"] == "WARNING"
    assert warning["channel_id"] == "c1"
    assert warning["thread_id"] == "invalid-time"
    assert warning["message_id"] == "invalid-time"


def test_file_only_post_reaches_generic_handler(tmp_path):
    instance, _, _, handler = listener(tmp_path, state=False)
    item = post("file", 1, message="")
    item["file_ids"] = ["f1"]
    run(run_session(instance, event(item)))
    assert [call[0] for call in handler.calls] == ["file"]


@pytest.mark.parametrize("post_type", ["system_join_channel", "system_header_change"])
def test_system_message_variants_are_marked_without_handler(tmp_path, post_type):
    instance, _, store, handler = listener(tmp_path)
    run(run_session(instance, event(post(post_type, 1, post_type=post_type))))
    assert handler.calls == []
    assert store.is_post_processed(post_type)


def test_post_lifecycle_logs_have_identity_and_correlation(tmp_path, capsys):
    instance, _, _, _ = listener(tmp_path, state=False)
    run(run_session(instance, event(post("logged", 1))))
    records = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    lifecycle = [record for record in records if record["event"] in {
        "post alindi", "post isleme basladi", "post isleme bitti"}]
    assert [record["event"] for record in lifecycle] == [
        "post alindi", "post isleme basladi", "post isleme bitti"]
    assert all(record["channel_id"] == "c1" and record["message_id"] == "logged"
               and record["correlation_id"] for record in lifecycle)
