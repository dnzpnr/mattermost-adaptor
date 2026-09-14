from __future__ import annotations

import os
import sqlite3

import pytest

from mattermost_adapter.api import MattermostAdapter
from mattermost_adapter.models import ConversationRef
from mattermost_adapter.normalize import extract_channel_and_thread
from mattermost_adapter.session import ThreadSessionResolver
from mattermost_adapter.state import SqliteStateStore


def test_root_and_reply_thread_extraction_is_preserved():
    assert extract_channel_and_thread({"channel_id": "c", "id": "root", "root_id": ""}) == ("c", "root")
    assert extract_channel_and_thread({"channel_id": "c", "id": "reply", "root_id": "root"}) == ("c", "root")
    with pytest.raises(ValueError):
        extract_channel_and_thread({"id": "root", "root_id": ""})


def test_default_session_is_thread_scoped_and_never_user_scoped():
    resolver = ThreadSessionResolver()
    a = resolver.resolve(ConversationRef("mattermost", "c1", "same"))
    b = resolver.resolve(ConversationRef("mattermost", "c2", "same"))
    assert a.session_id == b.session_id == "mattermost:same"
    assert "user" not in a.session_id


def test_unmapped_resolver_fails_closed_after_own_and_system_filters():
    class RejectAll:
        def resolve(self, _ref):
            return None

    adapter = MattermostAdapter(resolver=RejectAll())
    base = {"id": "p", "channel_id": "c", "root_id": "", "user_id": "u",
            "message": "hello", "create_at": 1, "type": ""}
    assert adapter.normalize_event(base).ignore_reason == "unmapped_conversation"
    assert adapter.normalize_event({**base, "user_id": "bot"}, bot_user_id="bot").ignore_reason == "own_message"
    assert adapter.normalize_event({**base, "type": "system_join_channel"}).ignore_reason == "system_message"


def test_state_file_is_0600_and_uses_english_tables(tmp_path):
    path = tmp_path / "state.db"
    previous = os.umask(0o777)
    try:
        SqliteStateStore(path, ["c1"])
    finally:
        os.umask(previous)
    assert path.stat().st_mode & 0o777 == 0o600
    with sqlite3.connect(path) as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"configured_channels", "channel_state", "processed_posts"} <= tables


def test_state_dedupe_and_cursor_only_moves_forward(tmp_path):
    store = SqliteStateStore(tmp_path / "state.db", ["c1"])
    store.initialize_cursor("c1", "p0", 10)
    store.initialize_cursor("c1", "ignored", 100)
    store.mark_post_processed("c1", "p2", 30)
    store.mark_post_processed("c1", "p1", 20)
    store.mark_post_processed("c1", "p3", 30)
    assert store.list_channels() == ["c1"]
    assert store.is_post_processed("p1") and store.is_post_processed("p2")
    assert store.get_last_post_id("c1") == "p3"

