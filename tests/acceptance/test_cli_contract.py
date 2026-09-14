"""Kabul 7-9 + güvenlik: JSON zarfı, kararlı çıkış kodları, sır sızmaması,
LibreChat/MindAlert bağımsızlığı."""
from __future__ import annotations

import json
import pathlib
import re

import pytest

from .conftest import REPO, SRC, envelope, run_cli
from .fake_mattermost import VALID_TOKEN, make_post


def _error(proc, code, exit_code):
    assert proc.returncode == exit_code, (proc.returncode, proc.stdout, proc.stderr)
    doc = envelope(proc)
    assert doc["status"] == "error" and doc["data"] is None
    assert doc["error"]["code"] == code
    assert isinstance(doc["error"]["message"], str) and doc["error"]["message"]
    return doc


def test_invalid_json_input_is_invalid_input_exit_2():
    _error(run_cli(["normalize-event"], stdin="{not json"), "INVALID_INPUT", 2)


def test_post_without_channel_is_invalid_input_exit_2():
    _error(run_cli(["normalize-event"], stdin={"id": "p1", "root_id": ""}), "INVALID_INPUT", 2)


def test_unknown_command_still_returns_json_envelope_exit_2():
    _error(run_cli(["no-such-command"]), "USAGE_ERROR", 2)


def test_send_message_empty_text_is_invalid_input(fake):
    _error(run_cli(["send-message"], env=fake.env(), stdin={"channel_id": "c1", "text": "  "}),
           "INVALID_INPUT", 2)
    assert fake.created_posts == []


@pytest.mark.parametrize("token", [None, "", "   "])
def test_missing_token_fails_safely_before_any_network_call(fake, token):
    _error(run_cli(["send-message"], env=fake.env(token=token),
                   stdin={"channel_id": "c1", "thread_id": "p1", "text": "x"}), "CONFIG_ERROR", 3)
    assert fake.requests == []


def test_invalid_numeric_config_is_config_error(fake):
    env = {**fake.env(), "MM_REQUEST_TIMEOUT_SECONDS": "nan"}
    _error(run_cli(["health", "--check-connection"], env=env), "CONFIG_ERROR", 3)


def test_rejected_token_is_auth_error_exit_4_and_token_never_leaks(fake):
    bad = "tok-WRONG-abcdef123456"
    proc = run_cli(["send-message"], env={**fake.env(token=bad), "MM_ADAPTER_LOG_LEVEL": "DEBUG"},
                   stdin={"channel_id": "c1", "thread_id": "p1", "text": "x"})
    _error(proc, "AUTH_ERROR", 4)
    for stream in (proc.stdout, proc.stderr):
        assert bad not in stream
        assert "Bearer " not in stream


def test_server_error_is_mattermost_api_error_exit_5_and_token_never_leaks(fake):
    fake.fail_create_post_status = 500
    proc = run_cli(["send-message"], env={**fake.env(), "MM_ADAPTER_LOG_LEVEL": "DEBUG"},
                   stdin={"channel_id": "c1", "thread_id": "p1", "text": "x"})
    _error(proc, "MATTERMOST_API_ERROR", 5)
    for stream in (proc.stdout, proc.stderr):
        assert VALID_TOKEN not in stream
        assert "Bearer " not in stream


def test_unreachable_server_is_mattermost_api_error(fake):
    env = {**fake.env(), "MM_PORT": "1", "MM_REQUEST_TIMEOUT_SECONDS": "2"}
    _error(run_cli(["send-message"], env=env, stdin={"channel_id": "c1", "text": "x"}),
           "MATTERMOST_API_ERROR", 5)


def test_stderr_logs_never_pollute_stdout_even_at_debug(fake):
    env = {**fake.env(), "MM_ADAPTER_LOG_LEVEL": "DEBUG"}
    proc = run_cli(["send-message"], env=env,
                   stdin={"channel_id": "c1", "thread_id": "p1", "text": "x"})
    assert proc.returncode == 0, proc.stderr
    envelope(proc)  # tek JSON nesnesi
    assert VALID_TOKEN not in proc.stderr


def test_operation_logs_carry_trace_identifiers(fake):
    env = {**fake.env(), "MM_ADAPTER_LOG_LEVEL": "INFO"}
    proc = run_cli(["send-message"], env=env,
                   stdin={"channel_id": "c1", "thread_id": "p1", "text": "x"})
    assert proc.returncode == 0, proc.stderr
    records = [json.loads(line) for line in proc.stderr.splitlines() if line.startswith("{")]
    assert records, proc.stderr
    traced = [r for r in records if r.get("channel_id") == "c1" and r.get("thread_id") == "p1"]
    assert traced, records
    assert all(r.get("provider") == "mattermost" and r.get("correlation_id") for r in traced)


def test_standalone_cli_needs_no_librechat_or_mindalert_modules(workdir):
    probe = (
        "import json, runpy, sys, importlib, pkgutil\n"
        "import mattermost_adapter\n"
        "for m in pkgutil.walk_packages(mattermost_adapter.__path__, 'mattermost_adapter.'):\n"
        "    importlib.import_module(m.name)\n"
        "bad = sorted(n for n in sys.modules if n.split('.')[0] in {\n"
        "    'bridge','yonetim','hafiza','mattermost_bot','tenant_resolver','celiski',\n"
        "    'mcp_sunucu','ingestion','librechat','evaluation','httpx','mcp'})\n"
        "print(json.dumps(bad))\n"
    )
    import subprocess, sys
    from .conftest import clean_env
    proc = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True,
                          env=clean_env(), cwd=workdir, timeout=60)
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout.strip().splitlines()[-1]) == []


def test_source_has_no_librechat_or_runtime_coupling():
    offenders = []
    for path in SRC.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for pattern in (r"librechat", r"\bhermes\b", r"\bpi_agent\b", r"plugin_manager",
                        r"MindAlert", r"yonetim", r"hafiza"):
            if re.search(pattern, text, re.IGNORECASE):
                offenders.append((str(path.relative_to(REPO)), pattern))
    assert offenders == []
