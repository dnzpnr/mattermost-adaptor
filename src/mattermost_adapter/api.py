from __future__ import annotations

import signal
import threading
from typing import Any

from .config import AdapterConfig
from .errors import InvalidInputError
from .listener import MattermostListener, MessageHandler
from .logging import configure, correlation_id, new_correlation_id
from .models import ConversationRef, NormalizationResult, OutboundMessage
from .normalize import normalize_post, parse_event
from .session import SessionResolver, ThreadSessionResolver
from .state import StateStore
from .transport import MattermostTransport, build_driver, run_with_reconnect


class MattermostAdapter:
    def __init__(self, config: AdapterConfig | None = None, *,
                 resolver: SessionResolver | None = None,
                 state_store: StateStore | None = None,
                 transport: MattermostTransport | None = None) -> None:
        self.config = config
        self.resolver = resolver or ThreadSessionResolver()
        self.state_store = state_store
        self._transport = transport
        if config is not None:
            configure(config.log_level, secrets=[config.token])

    def normalize_event(self, payload: Any, *, bot_user_id: str | None = None) -> NormalizationResult:
        post, event_data = parse_event(payload)
        if post is None:
            return NormalizationResult("ignore", "not_posted_event", None, None)
        message = normalize_post(post, event_data)
        if bot_user_id is not None and post.get("user_id") == bot_user_id:
            return NormalizationResult("ignore", "own_message", message, None)
        if str(post.get("type") or "").startswith("system_"):
            return NormalizationResult("ignore", "system_message", message, None)
        ref = ConversationRef(provider="mattermost", channel_id=message.channel_id,
                              thread_id=message.thread_id)
        session = self.resolver.resolve(ref)
        if session is None:
            return NormalizationResult("ignore", "unmapped_conversation", message, None)
        if not message.attachments and not message.content.strip():
            return NormalizationResult("ignore", "empty_message", message, session)
        return NormalizationResult("process", None, message, session)

    def _get_transport(self) -> MattermostTransport:
        if self._transport is None:
            if self.config is None:
                raise InvalidInputError("Mattermost configuration is required for this operation")
            self._transport = MattermostTransport(build_driver(self.config),
                                                  reply_prop_key=self.config.reply_prop_key)
        return self._transport

    def send_message(self, payload: Any):
        if not isinstance(payload, dict):
            raise InvalidInputError("Input must be a JSON object")
        channel_id = payload.get("channel_id")
        text = payload.get("text")
        if not isinstance(channel_id, str) or not channel_id.strip():
            raise InvalidInputError("channel_id must be a non-empty string")
        if not isinstance(text, str) or not text.strip():
            raise InvalidInputError("text must be a non-empty string")
        props = payload.get("props", {})
        if not isinstance(props, dict):
            raise InvalidInputError("props must be a JSON object")
        thread_id = payload.get("thread_id")
        reply_to = payload.get("reply_to_message_id")
        if thread_id is not None and not isinstance(thread_id, str):
            raise InvalidInputError("thread_id must be a string")
        if reply_to is not None and not isinstance(reply_to, str):
            raise InvalidInputError("reply_to_message_id must be a string")
        trace_id = new_correlation_id()
        token = correlation_id.set(trace_id)
        try:
            return self._get_transport().send_message(OutboundMessage(
                channel_id=channel_id, text=text, thread_id=thread_id,
                reply_to_message_id=reply_to, props=props))
        finally:
            correlation_id.reset(token)

    def fetch_file(self, file_id: str, output_dir: str):
        if not file_id:
            raise InvalidInputError("file_id cannot be empty")
        if not output_dir:
            raise InvalidInputError("output_dir cannot be empty")
        return self._get_transport().fetch_file(file_id, output_dir)

    def health(self, *, check_connection: bool = False) -> dict[str, Any]:
        if self.config is None:
            raise InvalidInputError("Mattermost configuration is required")
        connection = None
        if check_connection:
            transport = self._get_transport()
            connection = {"ok": True, "bot_user_id": transport.bot_user_id,
                          "username": transport.username}
        return {"config": self.config.public_dict(), "connection": connection}

    def listen(self, handler: MessageHandler, *, install_signal_handlers: bool = False) -> None:
        if self.config is None:
            raise InvalidInputError("Mattermost configuration is required")
        state = self.state_store
        stopping = threading.Event()
        current: list[MattermostListener | None] = [None]

        def request_stop(_signum=None, _frame=None):
            stopping.set()
            if current[0] is not None:
                current[0].request_stop()

        previous = {}
        if install_signal_handlers:
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous[signum] = signal.signal(signum, request_stop)

        def factory(transport: MattermostTransport) -> MattermostListener:
            listener = MattermostListener(
                transport, self.resolver, handler, state,
                queue_capacity=self.config.queue_capacity,
                drain_deadline_seconds=self.config.drain_deadline,
            )
            current[0] = listener
            return listener

        try:
            run_with_reconnect(self.config, factory, stop_requested=stopping.is_set)
        finally:
            if install_signal_handlers:
                for signum, old_handler in previous.items():
                    signal.signal(signum, old_handler)
