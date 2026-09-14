from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from .errors import ConfigError


def _positive_float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name, str(default))
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{name} must be a positive finite number: {raw!r}") from exc
    if not math.isfinite(value) or value <= 0:
        raise ConfigError(f"{name} must be a positive finite number: {raw!r}")
    return value


def _port(env: Mapping[str, str]) -> int:
    raw = env.get("MM_PORT", "8065")
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"MM_PORT must be an integer: {raw!r}") from exc
    if not 0 < value <= 65535:
        raise ConfigError(f"MM_PORT must be between 1 and 65535: {raw!r}")
    return value


def _queue_capacity(env: Mapping[str, str]) -> int:
    raw = env.get("MM_BRIDGE_QUEUE_CAPACITY", "50")
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"MM_BRIDGE_QUEUE_CAPACITY must be an integer with 0 < value < 100: {raw!r}") from exc
    if value <= 0 or value >= 100:
        raise ConfigError(f"MM_BRIDGE_QUEUE_CAPACITY must satisfy 0 < value < 100: {raw!r}")
    return value


def _default_state_path(env: Mapping[str, str]) -> str:
    explicit = env.get("MM_ADAPTER_STATE_PATH")
    if explicit:
        return explicit
    base = env.get("XDG_STATE_HOME")
    if base:
        return str(Path(base) / "mattermost-adapter" / "state.db")
    return str(Path.home() / ".local" / "state" / "mattermost-adapter" / "state.db")


@dataclass(frozen=True)
class AdapterConfig:
    url: str = "localhost"
    port: int = 8065
    scheme: str = "http"
    token: str = field(default="", repr=False)
    request_timeout: float = 30.0
    websocket_recv_gap_threshold: float = 60.0
    event_loop_lag_threshold: float = 1.0
    event_loop_watchdog_heartbeat: float = 300.0
    queue_capacity: int = 50
    drain_deadline: float = 180.0
    state_path: str = ""
    channels: tuple[str, ...] = ()
    reply_prop_key: str = "reply_to_post_id"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None, *, require_token: bool = True) -> "AdapterConfig":
        env = os.environ if environ is None else environ
        token = str(env.get("MM_TOKEN", "")).strip()
        if require_token and not token:
            raise ConfigError("MM_TOKEN is missing or contains only whitespace")
        scheme = str(env.get("MM_SCHEME", "http")).strip().lower()
        if scheme not in {"http", "https"}:
            raise ConfigError("MM_SCHEME must be 'http' or 'https'")
        channels = tuple(dict.fromkeys(part.strip() for part in env.get("MM_ADAPTER_CHANNELS", "").split(",") if part.strip()))
        reply_key = str(env.get("MM_ADAPTER_REPLY_PROP_KEY", "reply_to_post_id")).strip()
        if not reply_key:
            raise ConfigError("MM_ADAPTER_REPLY_PROP_KEY cannot be empty")
        level = str(env.get("MM_ADAPTER_LOG_LEVEL", "INFO")).strip().upper()
        if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ConfigError(f"MM_ADAPTER_LOG_LEVEL is invalid: {level!r}")
        return cls(
            url=str(env.get("MM_URL", "localhost")).strip() or "localhost",
            port=_port(env), scheme=scheme, token=token,
            request_timeout=_positive_float(env, "MM_REQUEST_TIMEOUT_SECONDS", 30.0),
            websocket_recv_gap_threshold=_positive_float(env, "MM_WEBSOCKET_RECV_GAP_THRESHOLD_SECONDS", 60.0),
            event_loop_lag_threshold=_positive_float(env, "MM_EVENT_LOOP_LAG_THRESHOLD_SECONDS", 1.0),
            event_loop_watchdog_heartbeat=_positive_float(env, "MM_EVENT_LOOP_WATCHDOG_HEARTBEAT_SECONDS", 300.0),
            queue_capacity=_queue_capacity(env),
            drain_deadline=_positive_float(env, "MM_BRIDGE_DRAIN_DEADLINE_SECONDS", 180.0),
            state_path=_default_state_path(env), channels=channels, reply_prop_key=reply_key,
            log_level=level,
        )

    def __repr__(self) -> str:
        values = [f"{name}={getattr(self, name)!r}" for name in self.__dataclass_fields__ if name != "token"]
        values.insert(3, "token='***'" if self.token else "token=''" )
        return f"AdapterConfig({', '.join(values)})"

    def public_dict(self) -> dict[str, object]:
        return {"url": self.url, "port": self.port, "scheme": self.scheme,
                "token_present": bool(self.token)}

