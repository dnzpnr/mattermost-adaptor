from __future__ import annotations

from typing import Protocol

from .models import ConversationRef, Session


class SessionResolver(Protocol):
    def resolve(self, ref: ConversationRef) -> Session | None: ...


class ThreadSessionResolver:
    def resolve(self, ref: ConversationRef) -> Session:
        return Session(session_id=f"mattermost:{ref.thread_id}", attributes={})

