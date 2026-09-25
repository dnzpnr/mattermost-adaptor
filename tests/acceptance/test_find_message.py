"""Kabul: gönderim belirsizliği çözülebilmeli.

Bir istemci `send-message` çağırıp yanıtı alamazsa (zaman aşımı, çökme) postun
oluşup oluşmadığını bilemez. Mesaj kimliği de elinde yoktur — kimlikle arama bu
vakayı çözmez. Çözüm, gönderimden ÖNCE istemcinin koyduğu bir anahtarı aramaktır.
"""
from __future__ import annotations

from .conftest import envelope, run_cli
from .fake_mattermost import make_post


def test_a_post_sent_with_a_client_key_can_be_found_again(fake):
    """Asıl vaka: istemci yanıtı alamadı, postun var olup olmadığını soruyor."""
    gonderim = run_cli(["send-message"], env=fake.env(), stdin={
        "channel_id": "c1", "text": "Proaktif uyari",
        "props": {"client_key": "req-42"},
    })
    assert gonderim.returncode == 0, gonderim.stderr

    proc = run_cli(["find-message"], env=fake.env(), stdin={
        "channel_id": "c1", "key": "req-42"})

    assert proc.returncode == 0, proc.stderr
    doc = envelope(proc)
    assert doc["status"] == "success", doc
    assert doc["data"]["found"] is True, doc
    assert doc["data"]["message_id"] == "created-1", doc


def test_an_unsent_key_is_reported_as_not_found_not_as_an_error(fake):
    """Bulunamamak bir hata değildir; istemcinin yeniden denemesi gereken bilgidir."""
    proc = run_cli(["find-message"], env=fake.env(), stdin={
        "channel_id": "c1", "key": "hic-gonderilmedi"})

    assert proc.returncode == 0, proc.stderr
    doc = envelope(proc)
    assert doc["status"] == "success", doc
    assert doc["data"]["found"] is False, doc
    assert doc["data"].get("message_id") in (None, ""), doc


def test_a_key_from_another_channel_is_not_reported_as_found(fake):
    """Kanal kapsamı korunur; başka kanaldaki post bu kanalın yanıtı değildir."""
    baska = make_post("p9", channel_id="c2", user_id="u1", create_at=1789380000000)
    baska["props"] = {"client_key": "req-77"}
    fake.add_offline_post(baska)

    proc = run_cli(["find-message"], env=fake.env(), stdin={
        "channel_id": "c1", "key": "req-77"})

    doc = envelope(proc)
    assert doc["status"] == "success", doc
    assert doc["data"]["found"] is False, doc


def test_a_missing_key_is_a_usage_error_not_a_silent_false(fake):
    """Eksik girdi `found: false` gibi görünmemeli; sessiz yanlış cevap üretirdi."""
    proc = run_cli(["find-message"], env=fake.env(), stdin={"channel_id": "c1"})

    doc = envelope(proc)
    assert doc["status"] == "error", doc
    assert doc["error"]["code"], doc


def test_the_search_does_not_leak_the_token(fake):
    """Aracın kuralı: sır hiçbir çıktıya girmez."""
    from .fake_mattermost import VALID_TOKEN

    proc = run_cli(["find-message"], env=fake.env(), stdin={
        "channel_id": "c1", "key": "req-42"})

    assert VALID_TOKEN not in (proc.stdout + proc.stderr)
