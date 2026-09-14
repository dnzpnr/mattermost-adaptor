from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True)
class Sender:
    id: str
    display_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "display_name": self.display_name}


@dataclass(frozen=True)
class Attachment:
    id: str
    name: str | None = None
    size: int | None = None
    mime_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "name": self.name, "size": self.size, "mime_type": self.mime_type}


@dataclass(frozen=True)
class NormalizedMessage:
    provider: str
    channel_id: str
    thread_id: str
    message_id: str
    parent_message_id: str | None
    sender: Sender
    content: str
    attachments: tuple[Attachment, ...] = ()
    timestamp: str | None = None
    raw_metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "channel_id": self.channel_id,
            "thread_id": self.thread_id,
            "message_id": self.message_id,
            "parent_message_id": self.parent_message_id,
            "sender": self.sender.to_dict(),
            "content": self.content,
            "attachments": [item.to_dict() for item in self.attachments],
            "timestamp": self.timestamp,
            "raw_metadata": self.raw_metadata,
        }


@dataclass(frozen=True)
class ConversationRef:
    provider: str
    channel_id: str
    thread_id: str

    def to_dict(self) -> dict[str, str]:
        return {"provider": self.provider, "channel_id": self.channel_id, "thread_id": self.thread_id}


@dataclass(frozen=True)
class Session:
    session_id: str
    attributes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"session_id": self.session_id, "attributes": self.attributes}


@dataclass(frozen=True)
class OutboundMessage:
    channel_id: str
    text: str
    thread_id: str | None = None
    reply_to_message_id: str | None = None
    props: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SentMessage:
    message_id: str
    channel_id: str
    thread_id: str | None
    create_at: int | None

    def to_dict(self) -> dict[str, Any]:
        return {"message_id": self.message_id, "channel_id": self.channel_id,
                "thread_id": self.thread_id, "create_at": self.create_at}


@dataclass(frozen=True)
class FileInfo:
    file_id: str
    name: str
    size: int | None
    mime_type: str | None
    path: str

    def to_dict(self) -> dict[str, Any]:
        return {"file_id": self.file_id, "name": self.name, "size": self.size,
                "mime_type": self.mime_type, "path": self.path}


@dataclass(frozen=True)
class NormalizationResult:
    action: Literal["process", "ignore"]
    ignore_reason: str | None
    message: NormalizedMessage | None
    session: Session | None

    def to_dict(self) -> dict[str, Any]:
        return {"action": self.action, "ignore_reason": self.ignore_reason,
                "message": self.message.to_dict() if self.message else None,
                "session": self.session.to_dict() if self.session else None}

