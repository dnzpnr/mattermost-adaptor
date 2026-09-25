from __future__ import annotations

import signal
import threading
from typing import Any

from .config import AdapterConfig
from .errors import InvalidInputError
from .listener import MattermostListener, MessageHandler
from .logging import configure, correlation_id, new_correlation_id
from .models import ConversationRef, FoundMessage, NormalizationResult, OutboundMessage
from .normalize import normalize_post, parse_event
from .session import SessionResolver, ThreadSessionResolver
from .state import StateStore
from .transport import MattermostTransport, build_driver, run_with_reconnect


class MattermostAdapter:
    DEFAULT_FIND_LIMIT = 200
    MAX_FIND_LIMIT = 200
    DEFAULT_FIND_PROP = "client_key"

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

    def find_message(self, payload: Any) -> FoundMessage:
        if not isinstance(payload, dict):
            raise InvalidInputError("Input must be a JSON object")
        channel_id = payload.get("channel_id")
        key = payload.get("key")
        if not isinstance(channel_id, str) or not channel_id.strip():
            raise InvalidInputError("channel_id must be a non-empty string")
        if not isinstance(key, str) or not key.strip():
            raise InvalidInputError("key must be a non-empty string")
        prop = payload.get("prop", self.DEFAULT_FIND_PROP)
        if not isinstance(prop, str) or not prop.strip():
            raise InvalidInputError("prop must be a non-empty string")
        limit = payload.get("limit", self.DEFAULT_FIND_LIMIT)
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise InvalidInputError("limit must be a positive integer")
        limit = min(limit, self.MAX_FIND_LIMIT)
        trace_id = new_correlation_id()
        token = correlation_id.set(trace_id)
        try:
            response = self._get_transport().get_posts_for_channel(channel_id, per_page=limit)
        finally:
            correlation_id.reset(token)
        posts = response.get("posts") if isinstance(response, dict) else None
        order = response.get("order") if isinstance(response, dict) else None
        posts = posts if isinstance(posts, dict) else {}
        for post_id in order or []:
            post = posts.get(post_id)
            if not isinstance(post, dict) or post.get("channel_id") != channel_id:
                continue
            props = post.get("props")
            if isinstance(props, dict) and props.get(prop) == key:
                return FoundMessage(
                    found=True,
                    message_id=str(post.get("id") or post_id),
                    channel_id=channel_id,
                    thread_id=str(post.get("root_id") or post.get("id") or post_id) or None,
                    create_at=post.get("create_at"),
                )
        return FoundMessage(found=False)

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
