"""Kabul 1-4 + 6: thread kimliği oturumu belirler, kullanıcı kimliği asla.

İddialar CLI'nin gerçek çıktısı üzerindedir: plugin'in göreceği `session_id`.
"""
from __future__ import annotations

import json

from .conftest import envelope, run_cli
from .fake_mattermost import make_post


def _posted(post, sender_name=None):
    data = {"post": json.dumps(post), "channel_type": "O", "team_id": "t1"}
    if sender_name:
        data["sender_name"] = sender_name
    return {"event": "posted", "data": data, "broadcast": {"channel_id": post["channel_id"]}, "seq": 3}


def _session(post, **kw):
    proc = run_cli(["normalize-event"], stdin=_posted(post), **kw)
    assert proc.returncode == 0, proc.stderr
    doc = envelope(proc)
    assert doc["status"] == "success"
    assert doc["data"]["action"] == "process", doc
    return doc["data"]["session"]["session_id"], doc["data"]["message"]


def test_new_thread_root_post_maps_to_one_session_keyed_by_its_own_id():
    session_id, message = _session(make_post("p1", create_at=1789380000000))
    assert session_id == "mattermost:p1"
    assert message["thread_id"] == "p1"
    assert message["parent_message_id"] is None


def test_messages_from_different_users_in_same_thread_share_the_session():
    root = _session(make_post("p1", user_id="u1", create_at=1789380000000))[0]
    r2 = _session(make_post("p2", root_id="p1", user_id="u2", create_at=1789380001000))[0]
    r3 = _session(make_post("p3", root_id="p1", user_id="u3", create_at=1789380002000))[0]
    r4 = _session(make_post("p4", root_id="p1", user_id="u1", create_at=1789380003000))[0]
    assert root == r2 == r3 == r4 == "mattermost:p1"


def test_different_thread_resolves_to_different_session_even_for_same_user_and_channel():
    a = _session(make_post("p1", user_id="u1", create_at=1789380000000))[0]
    b = _session(make_post("p9", user_id="u1", create_at=1789380009000))[0]
    b_reply = _session(make_post("p10", root_id="p9", user_id="u1", create_at=1789380010000))[0]
    assert a != b
    assert b == b_reply == "mattermost:p9"


def test_user_id_never_appears_in_session_identity():
    session_id, _ = _session(make_post("p2", root_id="p1", user_id="user-zzz", create_at=1))
    assert "user-zzz" not in session_id


def test_same_session_from_bare_post_and_websocket_event():
    post = make_post("p2", root_id="p1", user_id="u2", create_at=1789380001000)
    proc = run_cli(["normalize-event"], stdin=post)
    assert proc.returncode == 0, proc.stderr
    assert envelope(proc)["data"]["session"]["session_id"] == "mattermost:p1"


def test_normalized_message_contract_is_stable():
    post = make_post(
        "p2", channel_id="c1", root_id="p1", user_id="u2", message="Merhaba ekip",
        create_at=1789380000000, file_ids=["f1", "f2"],
        metadata={"files": [{"id": "f1", "name": "rapor.pdf", "size": 12,
                             "mime_type": "application/pdf"}]},
    )
    proc = run_cli(["normalize-event"], stdin=_posted(post, sender_name="@ayse"))
    assert proc.returncode == 0, proc.stderr
    doc = envelope(proc)
    assert doc == {
        "status": "success",
        "error": None,
        "data": {
            "action": "process",
            "ignore_reason": None,
            "session": {"session_id": "mattermost:p1", "attributes": {}},
            "message": {
                "provider": "mattermost",
                "channel_id": "c1",
                "thread_id": "p1",
                "message_id": "p2",
                "parent_message_id": "p1",
                "sender": {"id": "u2", "display_name": "@ayse"},
                "content": "Merhaba ekip",
                "attachments": [
                    {"id": "f1", "name": "rapor.pdf", "size": 12, "mime_type": "application/pdf"},
                    {"id": "f2", "name": None, "size": None, "mime_type": None},
                ],
                "timestamp": "2026-09-14T10:00:00.000Z",
                "raw_metadata": {"post_type": "", "create_at": 1789380000000,
                                 "channel_type": "O", "team_id": "t1", "props": {}},
            },
        },
    }
    # Alan sırası da sözleşmenin parçası (JSON tüketicileri için kararlı çıktı).
    assert list(json.loads(proc.stdout)["data"]["message"]) == [
        "provider", "channel_id", "thread_id", "message_id", "parent_message_id", "sender",
        "content", "attachments", "timestamp", "raw_metadata",
    ]


def test_ignore_rules_match_existing_bridge_filters():
    own = run_cli(["normalize-event", "--bot-user-id", "bot-1"],
                  stdin=_posted(make_post("p5", user_id="bot-1", create_at=1)))
    system = run_cli(["normalize-event"],
                     stdin=_posted(make_post("p6", post_type="system_join_channel", create_at=1)))
    empty = run_cli(["normalize-event"], stdin=_posted(make_post("p7", message="   ", create_at=1)))
    file_only = run_cli(["normalize-event"],
                        stdin=_posted(make_post("p8", message="", file_ids=["f9"], create_at=1)))
    other = run_cli(["normalize-event"], stdin={"event": "typing", "data": {}, "seq": 4})
    results = {name: envelope(p)["data"] for name, p in
               {"own": own, "system": system, "empty": empty, "file_only": file_only,
                "other": other}.items()}
    assert all(p.returncode == 0 for p in (own, system, empty, file_only, other))
    assert results["own"]["action"] == "ignore" and results["own"]["ignore_reason"] == "own_message"
    assert results["system"]["ignore_reason"] == "system_message"
    assert results["empty"]["ignore_reason"] == "empty_message"
    assert results["file_only"]["action"] == "process"
    assert results["other"] == {"action": "ignore", "ignore_reason": "not_posted_event",
                                "message": None, "session": None}
