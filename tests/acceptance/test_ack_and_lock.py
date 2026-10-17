"""Kabul (MindAlert M2 ön koşulu): teslim onayı (ack) ve tek-dinleyici kilidi. Sözleşmedir, işçi değiştirmez.

Ack kipi OPT-IN (`MM_ADAPTER_ACK=1`): her `message` kaydında `ack_id` vardır; imleç (replay/tekillik durumu) YALNIZ tüketici stdin'e
`{"ack":"<ack_id>"}` satırı yazınca ilerler. Onaylanmayan mesajlar süreç yeniden başlayınca TEKRAR çıkar (en-az-bir-kez). Bir ack, o mesaja
ve ondan ÖNCE teslim edilmiş tüm mesajlara geçerlidir (sıralı onay). Her ack stdout'a `{"type":"ack","ack_id":...,"status":"committed"}` satırı yazar
(bilinmeyen kimlik: `{"type":"ack_error","ack_id":...,"error":{"code":"UNKNOWN_ACK",...}}`, süreç düşmez).
Kilit: aynı durum dosyası için ikinci `listen` ağa çıkmadan stdout'a `{"type":"error","error":{"code":"ALREADY_RUNNING",...}}` yazar, çıkış kodu 6.
Kilit süreç ölünce (SIGKILL dahil) kendiliğinden düşer. Ack/kilit yokken mevcut davranış DEĞİŞMEZ.
"""
from __future__ import annotations

import json
import queue
import signal
import subprocess
import sys
import threading
import time

from .conftest import clean_env
from .fake_mattermost import make_post
from .test_listen import WAIT, ListenProcess, _wait_connected


class AckProcess(ListenProcess):
    def __init__(self, env):
        self.proc = subprocess.Popen([sys.executable, "-m", "mattermost_adapter", "listen"], stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=clean_env(env))
        self.lines = queue.Queue()
        self.stdout_lines, self.stderr_chunks = [], []
        threading.Thread(target=self._pump_out, daemon=True).start()
        threading.Thread(target=self._pump_err, daemon=True).start()

    def ack(self, ack_id):
        self.proc.stdin.write(json.dumps({"ack": ack_id}) + "\n")
        self.proc.stdin.flush()

    def wait_type(self, kind, count=1):
        out, deadline = [], time.monotonic() + WAIT
        while len(out) < count:
            remaining = deadline - time.monotonic()
            assert remaining > 0, f"{kind} gelmedi; stdout={self.stdout_lines[-5:]} stderr={self.stderr[-2000:]}"
            try:
                doc = json.loads(self.lines.get(timeout=remaining))
            except queue.Empty:
                continue
            if doc.get("type") == kind:
                out.append(doc)
        return out

    def stop(self):
        try:
            self.proc.stdin.close()
        except Exception:
            pass
        return super().stop()


def mid(doc):
    return doc["data"]["message"]["message_id"]


def env_for(fake, tmp_path, **extra):
    return {**fake.env(), "MM_ADAPTER_STATE_PATH": str(tmp_path / "state.db"), "MM_ADAPTER_CHANNELS": "c1", **extra}


def test_ack_mode_messages_carry_ack_ids_and_unacked_messages_replay_after_restart(fake, tmp_path):
    env = env_for(fake, tmp_path, MM_ADAPTER_ACK="1")
    first = AckProcess(env)
    try:
        _wait_connected(fake, first)
        fake.push_post(make_post("p1", user_id="u1", message="bir"))
        fake.push_post(make_post("p2", root_id="p1", user_id="u2", message="iki"))
        fake.push_post(make_post("p3", root_id="p1", user_id="u3", message="üç"))
        docs = first.messages(3)
        assert [mid(d) for d in docs] == ["p1", "p2", "p3"]
        ids = [d["ack_id"] for d in docs]
        assert all(isinstance(i, str) and i for i in ids) and len(set(ids)) == 3
        first.ack(ids[0])
        assert first.wait_type("ack")[0] == {"type": "ack", "ack_id": ids[0], "status": "committed"}
    finally:
        assert first.stop() == 0, first.stderr[-2000:]

    second = AckProcess(env)
    try:
        _wait_connected(fake, second)
        again = second.messages(2)
        assert [mid(d) for d in again] == ["p2", "p3"]       # p1 onaylanmıştı; p2,p3 onaysız -> tekrar
    finally:
        assert second.stop() == 0, second.stderr[-2000:]


def test_acking_a_later_message_commits_all_earlier_ones(fake, tmp_path):
    env = env_for(fake, tmp_path, MM_ADAPTER_ACK="1")
    first = AckProcess(env)
    try:
        _wait_connected(fake, first)
        fake.push_post(make_post("q1", user_id="u1"))
        fake.push_post(make_post("q2", root_id="q1", user_id="u2"))
        docs = first.messages(2)
        first.ack(docs[1]["ack_id"])
        first.wait_type("ack")
    finally:
        assert first.stop() == 0
    second = AckProcess(env)
    try:
        _wait_connected(fake, second)
        fake.push_post(make_post("q3", root_id="q1", user_id="u3"))
        assert [mid(d) for d in second.messages(1)] == ["q3"]   # q1,q2 tekrar ETMEZ
    finally:
        assert second.stop() == 0
    assert [mid(d) for d in second.all_message_docs()] == ["q3"]


def test_unknown_ack_is_reported_not_fatal(fake, tmp_path):
    lp = AckProcess(env_for(fake, tmp_path, MM_ADAPTER_ACK="1"))
    try:
        _wait_connected(fake, lp)
        lp.ack("yok-boyle-bir-id")
        err = lp.wait_type("ack_error")[0]
        assert err["ack_id"] == "yok-boyle-bir-id" and err["error"]["code"] == "UNKNOWN_ACK"
        fake.push_post(make_post("r1", user_id="u1"))
        assert [mid(d) for d in lp.messages(1)] == ["r1"]       # hâlâ yaşıyor
    finally:
        assert lp.stop() == 0


def test_without_ack_mode_cursor_advances_as_before_and_no_ack_id(fake, tmp_path):
    env = env_for(fake, tmp_path)
    first = ListenProcess(env)
    try:
        _wait_connected(fake, first)
        fake.push_post(make_post("n1", user_id="u1"))
        doc = first.messages(1)[0]
        assert "ack_id" not in doc
    finally:
        assert first.stop() == 0
    second = ListenProcess(env)
    try:
        _wait_connected(fake, second)
        fake.push_post(make_post("n2", root_id="n1", user_id="u2"))
        assert [mid(d) for d in second.messages(1)] == ["n2"]   # n1 tekrar etmez (eski davranış)
    finally:
        assert second.stop() == 0


def test_second_listener_on_the_same_state_file_is_refused_with_exit_6(fake, tmp_path):
    env = env_for(fake, tmp_path)
    first = ListenProcess(env)
    try:
        _wait_connected(fake, first)
        before = len(fake.requests)
        proc = subprocess.run([sys.executable, "-m", "mattermost_adapter", "listen"], capture_output=True, text=True,
                              env=clean_env(env), timeout=WAIT)
        assert proc.returncode == 6, proc.stderr[-2000:]
        last = json.loads(proc.stdout.strip().splitlines()[-1])
        assert last["type"] == "error" and last["error"]["code"] == "ALREADY_RUNNING"
        assert len(fake.requests) == before                      # ikinci süreç ağa hiç çıkmadı
        fake.push_post(make_post("l1", user_id="u1"))
        assert [mid(d) for d in first.messages(1)] == ["l1"]     # birincisi etkilenmedi
    finally:
        assert first.stop() == 0


def test_lock_is_released_when_the_holder_is_killed(fake, tmp_path):
    env = env_for(fake, tmp_path)
    first = ListenProcess(env)
    _wait_connected(fake, first)
    first.proc.send_signal(signal.SIGKILL)
    first.proc.wait(timeout=WAIT)
    second = ListenProcess(env)
    try:
        _wait_connected(fake, second)                            # kilit düştü, başlayabildi
    finally:
        assert second.stop() == 0


def test_different_state_files_do_not_block_each_other(fake, tmp_path):
    a = ListenProcess(env_for(fake, tmp_path))
    try:
        _wait_connected(fake, a)
        other = tmp_path / "other"
        other.mkdir()
        b = ListenProcess(env_for(fake, other))
        try:
            time.sleep(1.5)
            assert b.proc.poll() is None
        finally:
            b.stop()
    finally:
        a.stop()
