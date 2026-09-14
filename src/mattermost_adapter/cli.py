from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .api import MattermostAdapter
from .config import AdapterConfig
from .errors import AdapterError, InvalidInputError, UsageError
from .logging import configure, redact
from .models import NormalizedMessage, Session
from .state import SqliteStateStore


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise UsageError(message)


def _parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(prog="mattermost-adapter", add_help=True)
    commands = parser.add_subparsers(dest="command", required=True)
    normalize = commands.add_parser("normalize-event")
    normalize.add_argument("--bot-user-id")
    commands.add_parser("send-message")
    fetch = commands.add_parser("fetch-file")
    fetch.add_argument("--file-id", required=True)
    fetch.add_argument("--output-dir", required=True)
    health = commands.add_parser("health")
    health.add_argument("--check-connection", action="store_true")
    commands.add_parser("listen")
    return parser


def _read_json() -> Any:
    try:
        return json.load(sys.stdin)
    except (ValueError, UnicodeError) as exc:
        raise InvalidInputError("stdin must contain valid JSON") from exc


def _write_envelope(*, data=None, error: AdapterError | None = None) -> None:
    document = ({"status": "error", "data": None,
                 "error": {"code": error.code, "message": redact(error.message)}}
                if error else {"status": "success", "data": data, "error": None})
    print(json.dumps(document, ensure_ascii=False, separators=(",", ":")), flush=True)


class _NDJSONHandler:
    def handle_message(self, message: NormalizedMessage, session: Session, trace_id: str) -> None:
        result = {"action": "process", "ignore_reason": None,
                  "message": message.to_dict(), "session": session.to_dict()}
        line = {"type": "message", "correlation_id": trace_id, "data": result}
        print(json.dumps(line, ensure_ascii=False, separators=(",", ":")), flush=True)


def _run(args: argparse.Namespace) -> None:
    if args.command == "normalize-event":
        result = MattermostAdapter().normalize_event(_read_json(), bot_user_id=args.bot_user_id)
        _write_envelope(data=result.to_dict())
        return
    if args.command == "health" and not args.check_connection:
        config = AdapterConfig.from_env(require_token=False)
    else:
        config = AdapterConfig.from_env(require_token=True)
    configure(config.log_level, secrets=[config.token])
    adapter = MattermostAdapter(
        config,
        state_store=(SqliteStateStore(config.state_path, config.channels)
                     if args.command == "listen" else None),
    )
    if args.command == "send-message":
        _write_envelope(data=adapter.send_message(_read_json()).to_dict())
    elif args.command == "fetch-file":
        _write_envelope(data=adapter.fetch_file(args.file_id, args.output_dir).to_dict())
    elif args.command == "health":
        _write_envelope(data=adapter.health(check_connection=args.check_connection))
    elif args.command == "listen":
        adapter.listen(_NDJSONHandler(), install_signal_handlers=True)
    else:
        raise UsageError(f"unknown command: {args.command}")


def main(argv: list[str] | None = None) -> int:
    listen = bool(argv and argv[0] == "listen") if argv is not None else len(sys.argv) > 1 and sys.argv[1] == "listen"
    try:
        args = _parser().parse_args(argv)
        listen = args.command == "listen"
        _run(args)
        return 0
    except AdapterError as exc:
        if listen:
            print(json.dumps({"type": "error", "error": {"code": exc.code,
                  "message": redact(exc.message)}}, ensure_ascii=False, separators=(",", ":")), flush=True)
        else:
            _write_envelope(error=exc)
        return exc.exit_code
    except KeyboardInterrupt:
        return 0
    except BaseException as exc:
        error = AdapterError(f"Internal adapter error: {type(exc).__name__}")
        if listen:
            print(json.dumps({"type": "error", "error": {"code": error.code,
                  "message": error.message}}, separators=(",", ":")), flush=True)
        else:
            _write_envelope(error=error)
        return error.exit_code
