from __future__ import annotations

import json
import re
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)
_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
_level = 20
_secrets: set[str] = set()
_authorization = re.compile(r"(?i)(authorization\s*[:=]\s*)(?:bearer\s+)?[^\s,;)]+")
_bearer = re.compile(r"(?i)\bbearer\s+[^\s,;)]+")


def configure(level: str = "INFO", *, secrets: tuple[str, ...] | list[str] = ()) -> None:
    global _level, _secrets
    _level = _LEVELS.get(level.upper(), 20)
    _secrets = {secret for secret in secrets if secret}


def redact(value: Any) -> Any:
    if isinstance(value, str):
        result = value
        for secret in sorted(_secrets, key=len, reverse=True):
            result = result.replace(secret, "[REDACTED]")
        result = _authorization.sub(r"\1[REDACTED]", result)
        result = _bearer.sub("[REDACTED]", result)
        return result
    if isinstance(value, dict):
        return {key: ("[REDACTED]" if str(key).lower() == "authorization" else redact(item))
                for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    return value


def new_correlation_id() -> str:
    return str(uuid4())


def log(event: str, *, level: str = "INFO", **fields: Any) -> None:
    level = level.upper()
    if _LEVELS.get(level, 20) < _level:
        return
    record: dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "level": level,
        "event": event,
        "provider": "mattermost",
    }
    current = correlation_id.get()
    if current and "correlation_id" not in fields:
        record["correlation_id"] = current
    record.update(fields)
    print(json.dumps(redact(record), ensure_ascii=False, separators=(",", ":")), file=sys.stderr, flush=True)

