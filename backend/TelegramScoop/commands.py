"""What a channel post is asking for, if anything. Pure functions
(tests/test_telegram_commands.py).

Two requests are recognised, and only from the configured channel:

    https://example.com/some-article      a bare URL: fetch that story
    /scoop <topic>                        search the news for a topic

Everything else posted to the channel (the hourly headlines, replies, chat)
is ignored.
"""

import re
from dataclasses import dataclass

COMMAND = "/scoop"
_URL_ONLY = re.compile(r"^https?://\S+$")
_COMMAND = re.compile(rf"^{COMMAND}(?:@\w+)?(?:\s+(.*))?$", re.IGNORECASE | re.DOTALL)


@dataclass(frozen=True)
class Request:
    kind: str  # "url", "search", or "usage" for a bare /scoop
    text: str
    chat_id: int
    message_id: int


def parse(update: dict, channel: str):
    """The Request a channel post makes, or None when it makes none.

    channel is the configured chat: an @username or a numeric id as a string."""
    post = update.get("channel_post")
    if not post:
        return None
    chat = post.get("chat") or {}
    if not _is_channel(chat, channel):
        return None

    text = (post.get("text") or "").strip()
    kind, argument = _classify(text)
    if kind is None:
        return None
    return Request(kind=kind, text=argument, chat_id=int(chat["id"]),
                   message_id=int(post["message_id"]))


def _classify(text: str):
    if _URL_ONLY.match(text):
        return "url", text
    command = _COMMAND.match(text)
    if command:
        query = (command.group(1) or "").strip()
        return ("search", query) if query else ("usage", "")
    return None, ""


def _is_channel(chat: dict, channel: str) -> bool:
    if channel.startswith("@"):
        return (chat.get("username") or "").lower() == channel[1:].lower()
    return str(chat.get("id")) == channel
