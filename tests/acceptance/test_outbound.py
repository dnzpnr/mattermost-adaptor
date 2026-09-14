"""Kabul 5: cevaplar doğru Mattermost thread'ine gider (sahte sunucunun
gerçekten aldığı create_post gövdesi üzerinden)."""
from __future__ import annotations

import json
import pathlib

from .conftest import envelope, run_cli
from .fake_mattermost import VALID_TOKEN, make_post


def test_reply_to_a_normalized_reply_lands_in_the_thread_root_not_the_reply(fake):
    reply = make_post("p3", channel_id="c7", root_id="p1", user_id="u3", create_at=1789380000000)
    norm = envelope(run_cli(["normalize-event"], stdin=reply))["data"]["message"]

    proc = run_cli(["send-message"], env=fake.env(), stdin={
        "channel_id": norm["channel_id"],
        "thread_id": norm["thread_id"],
        "text": "Asistan cevabı",
        "reply_to_message_id": norm["message_id"],
    })
    assert proc.returncode == 0, proc.stderr
    doc = envelope(proc)
    assert doc["status"] == "success"
    assert doc["data"] == {"message_id": "created-1", "channel_id": "c7", "thread_id": "p1",
                           "create_at": 1789380000123}
    assert len(fake.created_posts) == 1
    sent = fake.created_posts[0]
    assert sent["channel_id"] == "c7"
    assert sent["root_id"] == "p1"
    assert sent["message"] == "Asistan cevabı"
    assert sent["props"]["reply_to_post_id"] == "p3"


def test_reply_prop_key_is_configurable_and_caller_props_are_kept(fake):
    env = {**fake.env(), "MM_ADAPTER_REPLY_PROP_KEY": "mindalert_cevap_post_id"}
    proc = run_cli(["send-message"], env=env, stdin={
        "channel_id": "c1", "thread_id": "p1", "text": "ok",
        "reply_to_message_id": "p2", "props": {"trace": "abc"}})
    assert proc.returncode == 0, proc.stderr
    assert fake.created_posts[0]["props"] == {"trace": "abc", "mindalert_cevap_post_id": "p2"}


def test_message_without_thread_is_posted_as_new_root(fake):
    proc = run_cli(["send-message"], env=fake.env(), stdin={"channel_id": "c1", "text": "duyuru"})
    assert proc.returncode == 0, proc.stderr
    assert fake.created_posts[0]["root_id"] == ""
    assert "reply_to_post_id" not in (fake.created_posts[0].get("props") or {})


def test_fetch_file_downloads_inside_output_dir_with_sanitized_unique_name(fake, tmp_path):
    fake.files["f1"] = ({"id": "f1", "name": "../../etc/rapor.pdf", "size": 5,
                        "mime_type": "application/pdf"}, b"%PDF-")
    out = tmp_path / "files"
    out.mkdir()
    first = run_cli(["fetch-file", "--file-id", "f1", "--output-dir", str(out)], env=fake.env())
    second = run_cli(["fetch-file", "--file-id", "f1", "--output-dir", str(out)], env=fake.env())
    assert first.returncode == 0 and second.returncode == 0, first.stderr + second.stderr
    d1, d2 = envelope(first)["data"], envelope(second)["data"]
    assert d1["name"] == "rapor.pdf" and d1["file_id"] == "f1"
    assert d1["size"] == 5 and d1["mime_type"] == "application/pdf"
    p1, p2 = pathlib.Path(d1["path"]), pathlib.Path(d2["path"])
    assert p1.parent.resolve() == out.resolve() and p2.parent.resolve() == out.resolve()
    assert p1.read_bytes() == b"%PDF-" and p2.read_bytes() == b"%PDF-"
    assert p1 != p2 and p2.name == "rapor (1).pdf"
    assert not (tmp_path / "etc").exists()


def test_health_reports_config_without_secrets_and_checks_connection(fake):
    offline = run_cli(["health"], env={k: v for k, v in fake.env().items() if k != "MM_TOKEN"})
    assert offline.returncode == 0, offline.stderr
    d = envelope(offline)["data"]
    assert d["config"]["token_present"] is False and d["connection"] is None

    online = run_cli(["health", "--check-connection"], env=fake.env())
    assert online.returncode == 0, online.stderr
    d = envelope(online)["data"]
    assert d["config"] == {"url": "127.0.0.1", "port": fake.port, "scheme": "http",
                           "token_present": True}
    assert d["connection"] == {"ok": True, "bot_user_id": "bot-user-id", "username": "assistant-bot"}
    assert VALID_TOKEN not in online.stdout + online.stderr
