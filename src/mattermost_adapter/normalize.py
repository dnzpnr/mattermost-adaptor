from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .errors import InvalidInputError
from .models import Attachment, NormalizedMessage, Sender


def extract_channel_and_thread(post: dict[str, Any]) -> tuple[str, str]:
    channel_id = post.get("channel_id")
    thread_id = post.get("root_id") or post.get("id")
    if not channel_id:
        raise ValueError("post has no channel_id")
    if not thread_id:
        raise ValueError("post has neither root_id nor id")
    return str(channel_id), str(thread_id)


def parse_event(payload: Any) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Return ``(post, event data)``; a non-posted websocket event has no post."""
    if not isinstance(payload, dict):
        raise InvalidInputError("Input must be a JSON object")
    if payload.get("event") is not None:
        if payload.get("event") != "posted":
            return None, {}
        data = payload.get("data")
        if not isinstance(data, dict):
            raise InvalidInputError("Posted event has no data object")
        post = data.get("post")
        if isinstance(post, str):
            try:
                post = json.loads(post)
            except (TypeError, ValueError) as exc:
                raise InvalidInputError("Posted event contains invalid post JSON") from exc
        if not isinstance(post, dict):
            raise InvalidInputError("Posted event has no post object")
        return post, data
    if "id" in payload or "channel_id" in payload:
        return payload, {}
    return None, {}


def _timestamp(create_at: Any) -> str | None:
    if create_at is None:
        return None
    try:
        value = float(create_at) / 1000
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    except (ValueError, TypeError, OverflowError, OSError) as exc:
        raise InvalidInputError("post.create_at must be a Unix timestamp in milliseconds") from exc
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def normalize_post(post: dict[str, Any], event_data: dict[str, Any] | None = None) -> NormalizedMessage:
    try:
        channel_id, thread_id = extract_channel_and_thread(post)
    except ValueError as exc:
        raise InvalidInputError(str(exc)) from exc
    post_id = post.get("id")
    if not post_id:
        raise InvalidInputError("post has no id")
    data = event_data or {}
    metadata = post.get("metadata") if isinstance(post.get("metadata"), dict) else {}
    files = metadata.get("files") if isinstance(metadata.get("files"), list) else []
    by_id = {str(item.get("id")): item for item in files if isinstance(item, dict) and item.get("id")}
    attachments = []
    for file_id in post.get("file_ids") or []:
        item = by_id.get(str(file_id), {})
        attachments.append(Attachment(id=str(file_id), name=item.get("name"), size=item.get("size"),
                                      mime_type=item.get("mime_type")))
    root_id = post.get("root_id") or None
    return NormalizedMessage(
        provider="mattermost",
        channel_id=channel_id,
        thread_id=thread_id,
        message_id=str(post_id),
        parent_message_id=str(root_id) if root_id else None,
        sender=Sender(id=str(post.get("user_id") or ""), display_name=data.get("sender_name")),
        content=str(post.get("message") or ""),
        attachments=tuple(attachments),
        timestamp=_timestamp(post.get("create_at")),
        raw_metadata={
            "post_type": post.get("type"),
            "create_at": post.get("create_at"),
            "channel_type": data.get("channel_type"),
            "team_id": data.get("team_id"),
            "props": post.get("props") if isinstance(post.get("props"), dict) else {},
        },
    )
