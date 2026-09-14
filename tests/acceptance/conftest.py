from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys

import pytest

from .fake_mattermost import FakeMattermost

REPO = pathlib.Path(__file__).resolve().parents[2]
SRC = REPO / "src"


def clean_env(extra: dict | None = None) -> dict:
    """MindAlert'in ya da kullanıcı ortamının sızmadığı minimal süreç ortamı."""
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "PYTHONPATH": str(SRC),
           "PYTHONDONTWRITEBYTECODE": "1", "LANG": "C.UTF-8"}
    env.update(extra or {})
    return env


def run_cli(args, *, stdin=None, env=None, cwd=None, timeout=60):
    proc = subprocess.run(
        [sys.executable, "-m", "mattermost_adapter", *args],
        input=stdin if stdin is None or isinstance(stdin, str) else json.dumps(stdin),
        capture_output=True, text=True, env=clean_env(env), cwd=cwd, timeout=timeout,
    )
    return proc


def envelope(proc):
    """stdout TAM OLARAK tek bir JSON nesnesi olmalı."""
    out = proc.stdout.strip()
    assert out, f"stdout boş; stderr={proc.stderr[-2000:]}"
    assert "\n" not in out, f"stdout tek satır/tek nesne değil: {proc.stdout!r}"
    doc = json.loads(out)
    assert set(doc) == {"status", "data", "error"}, doc
    return doc


@pytest.fixture
def fake():
    server = FakeMattermost().start()
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture
def workdir(tmp_path):
    d = tmp_path / "cwd"
    d.mkdir()
    return d
