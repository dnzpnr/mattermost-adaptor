"""Kabul 1-4 ve 10, gerçek gelen-mesaj yolu üzerinden: `listen` süreci sahte
sunucunun websocket'ine bağlanır, çok kullanıcılı thread'ler NDJSON olarak
çıkar; replay/tekillik süreç yeniden başlasa da korunur (MindAlert köprüsünün
dayanıklılık davranışı)."""
from __future__ import annotations

import json
import os
import queue
import signal
import subprocess
import sys
import threading
import time

from .conftest import clean_env
from .fake_mattermost import BOT_USER_ID, VALID_TOKEN, make_post

WAIT = 30


class ListenProcess:
    def __init__(self, env):
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "mattermost_adapter", "listen"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=clean_env(env),
        )
        self.lines: "queue.Queue[str]" = queue.Queue()
        self.stdout_lines: list[str] = []
        self.stderr_chunks: list[str] = []
        threading.Thread(target=self._pump_out, daemon=True).start()
        threading.Thread(target=self._pump_err, daemon=True).start()

    def _pump_out(self):
        for line in self.proc.stdout:
            self.stdout_lines.append(line)
            self.lines.put(line)

    def _pump_err(self):
        for line in self.proc.stderr:
            self.stderr_chunks.append(line)

    def messages(self, count):
        out = []
        deadline = time.monotonic() + WAIT
        while len(out) < count:
            remaining = deadline - time.monotonic()
            assert remaining > 0, f"yalnız {len(out)} mesaj geldi; stderr={self.stderr[-3000:]}"
            try:
                line = self.lines.get(timeout=remaining)
            except queue.Empty:
                continue
            doc = json.loads(line)
            if doc.get("type") == "message":
                out.append(doc)
        return out

    @property
    def stderr(self):
        return "".join(self.stderr_chunks)

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
        try:
            code = self.proc.wait(timeout=WAIT)
        finally:
            if self.proc.poll() is None:
                self.proc.kill()
        time.sleep(0.2)
        return code

    def all_message_docs(self):
        docs = [json.loads(line) for line in self.stdout_lines if line.strip()]
        return [d for d in docs if d.get("type") == "message"]


def _wait_connected(fake, lp):
    assert fake.ws_connected.wait(WAIT), f"listen bağlanmadı; stderr={lp.stderr[-3000:]}"


def _summary(doc):
    data = doc["data"]
    assert data["action"] == "process"
    assert doc["correlation_id"]
    return data["message"]["message_id"], data["session"]["session_id"], data["message"]["sender"]["id"]


def test_listen_streams_multi_user_threads_as_thread_sessions_and_survives_restart(fake, tmp_path):
    env = {**fake.env(), "MM_ADAPTER_STATE_PATH": str(tmp_path / "state.db"),
           "MM_ADAPTER_CHANNELS": "c1"}

    first = ListenProcess(env)
    try:
        _wait_connected(fake, first)
        fake.push_post(make_post("p1", user_id="u1", message="Thread A soru"), sender_name="@u1")
        fake.push_post(make_post("p2", root_id="p1", user_id="u2", message="ben de"))
        fake.push_post(make_post("b1", root_id="p1", user_id=BOT_USER_ID, message="bot cevabı"))
        fake.push_post(make_post("s1", user_id="u9", message="joined", post_type="system_join_channel"))
        fake.push_post(make_post("p3", root_id="p1", user_id="u3", message="ek bilgi"))
        fake.push_post(make_post("p4", user_id="u1", message="Thread B"))
        got = [_summary(d) for d in first.messages(4)]
    finally:
        code = first.stop()
    assert code == 0, first.stderr[-3000:]
    assert got == [
        ("p1", "mattermost:p1", "u1"),
        ("p2", "mattermost:p1", "u2"),
        ("p3", "mattermost:p1", "u3"),
        ("p4", "mattermost:p4", "u1"),
    ]
    # Botun kendi mesajı ve sistem mesajı hiç çıkmaz; hiçbir post iki kez çıkmaz.
    assert len(first.all_message_docs()) == 4
    assert VALID_TOKEN not in first.stderr

    # Adaptör kapalıyken thread B'ye yeni cevap düşer.
    fake.add_offline_post(make_post("p5", root_id="p4", user_id="u2", message="kaçırılan"))

    second = ListenProcess(env)
    try:
        _wait_connected(fake, second)
        replayed = [_summary(d) for d in second.messages(1)]
        fake.push_post(make_post("p6", root_id="p1", user_id="u4", message="canlı"))
        live = [_summary(d) for d in second.messages(1)]
    finally:
        code = second.stop()
    assert code == 0, second.stderr[-3000:]
    assert replayed == [("p5", "mattermost:p4", "u2")]
    assert live == [("p6", "mattermost:p1", "u4")]
    # p1..p4 zaten işlenmişti: yeniden çıkmaz.
    assert [_summary(d)[0] for d in second.all_message_docs()] == ["p5", "p6"]


def test_listen_first_start_does_not_replay_existing_channel_history(fake, tmp_path):
    fake.add_offline_post(make_post("old1", user_id="u1"))
    fake.add_offline_post(make_post("old2", root_id="old1", user_id="u2"))
    env = {**fake.env(), "MM_ADAPTER_STATE_PATH": str(tmp_path / "state.db"),
           "MM_ADAPTER_CHANNELS": "c1"}
    lp = ListenProcess(env)
    try:
        _wait_connected(fake, lp)
        fake.push_post(make_post("new1", root_id="old1", user_id="u3"))
        got = [_summary(d) for d in lp.messages(1)]
    finally:
        code = lp.stop()
    assert code == 0, lp.stderr[-3000:]
    assert got == [("new1", "mattermost:old1", "u3")]
    assert [_summary(d)[0] for d in lp.all_message_docs()] == ["new1"]


def test_listen_missing_token_emits_error_line_and_exits_3(fake):
    proc = subprocess.run([sys.executable, "-m", "mattermost_adapter", "listen"],
                          capture_output=True, text=True, env=clean_env(fake.env(token=None)),
                          timeout=WAIT)
    assert proc.returncode == 3, proc.stderr
    last = json.loads(proc.stdout.strip().splitlines()[-1])
    assert last["type"] == "error" and last["error"]["code"] == "CONFIG_ERROR"
    assert fake.requests == []


def test_listen_rejected_token_is_permanent_auth_error_exit_4(fake):
    bad = "tok-WRONG-zzzzzzzzzz"
    proc = subprocess.run([sys.executable, "-m", "mattermost_adapter", "listen"],
                          capture_output=True, text=True, env=clean_env(fake.env(token=bad)),
                          timeout=WAIT)
    assert proc.returncode == 4, proc.stderr[-3000:]
    last = json.loads(proc.stdout.strip().splitlines()[-1])
    assert last["type"] == "error" and last["error"]["code"] == "AUTH_ERROR"
    assert bad not in proc.stdout + proc.stderr
