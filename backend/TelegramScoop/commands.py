"""What a channel post is asking for, if anything. Pure functions
(tests/test_telegram_commands.py).

Six requests are recognised, and only from the configured channel. Only its
admins can post there, so only they can make them:

    https://www.2000.news/20261008/ccc10cac   a permalink: post that headline for grading
    https://example.com/some-article          any other bare URL: fetch that story
    /scoop <request>                          have the scoop editor find stories about it
    /scoop <url>                              the same as posting the URL bare
    /render <permalink> [<permalink>]         an Instagram post of the first headline,
                                              with the second as its second story
    /render or "rerender", posted as a reply  render the permalinks in the post it replies to
    a grading emoji, posted as a reply        grade the post it replies to
    any other text, posted as a reply         a note on the post it replies to

Everything else posted to the channel (the hourly headlines, chat) is ignored.
"""

import re
from dataclasses import dataclass

COMMAND = "/scoop"
RENDER_COMMAND = "/render"
EMOJI_GRADES = {
    "🔥": "outstanding",
    "👍": "solid",
    "🤣": "solid",
    "😐": "meh",
    "👎": "bad",
}
VARIATION_SELECTOR = "\ufe0f"
_URL_ONLY = re.compile(r"^https?://\S+$")
_PERMALINK = re.compile(r"^https?://(?:www\.)?2000\.news/(\d{8})/([0-9a-f]{8})/?(?:[?#]\S*)?$",
                        re.IGNORECASE)
_PERMALINK_IN_TEXT = re.compile(r"https?://(?:www\.)?2000\.news/(\d{8})/([0-9a-f]{8})(?![0-9a-z])",
                                re.IGNORECASE)
_COMMAND = re.compile(rf"^{COMMAND}(?:@\w+)?(?:\s+(.*))?$", re.IGNORECASE | re.DOTALL)
_RENDER_COMMAND = re.compile(rf"^{RENDER_COMMAND}(?:@\w+)?(?:\s+(.*))?$", re.IGNORECASE | re.DOTALL)
_RERENDER = re.compile(r"^(?:re-?)?render\W*$", re.IGNORECASE)


@dataclass(frozen=True)
class Request:
    kind: str  # "permalink", "url", "search", "render", "grade", "reply", or "usage" for a bare /scoop
    text: str  # for a grade, the grade the emoji stands for; for a render, its permalinks
    chat_id: int
    message_id: int
    reply_to_message_id: int = None


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
    reply_to_message = post.get("reply_to_message") or {}
    replied_to = reply_to_message.get("message_id")
    replied_to_text = reply_to_message.get("text") or reply_to_message.get("caption") or ""
    kind, argument = _classify(text, replied_to_text)
    if kind is None and text and replied_to is not None:
        kind, argument = _classify_reply(text, replied_to_text)
    if kind is None:
        return None
    return Request(kind=kind, text=argument, chat_id=int(chat["id"]),
                   message_id=int(post["message_id"]),
                   reply_to_message_id=int(replied_to) if replied_to is not None else None)


def permalink_key(url: str):
    """The (YearMonthDay, HeadlineId) a 2000.news permalink names, or None."""
    match = _PERMALINK.match(url)
    return (match.group(1), match.group(2).lower()) if match else None


def _classify(text: str, replied_to_text: str):
    render = _RENDER_COMMAND.match(text)
    if render:
        return "render", _render_targets(render.group(1) or replied_to_text)
    command = _COMMAND.match(text)
    if not command:
        return _classify_link(text)
    argument = (command.group(1) or "").strip()
    if not argument:
        return "usage", ""
    kind, link = _classify_link(argument)
    return (kind, link) if kind else ("search", argument)


def _classify_reply(text: str, replied_to_text: str):
    if _RERENDER.match(text):
        return "render", _render_targets(replied_to_text)
    grade = EMOJI_GRADES.get(text.replace(VARIATION_SELECTOR, ""))
    return ("grade", grade) if grade else ("reply", text)


def _render_targets(text: str) -> str:
    """Every 2000.news permalink in text, in order, spelled the canonical way."""
    return " ".join(f"https://www.2000.news/{day}/{headline_id.lower()}"
                    for day, headline_id in _PERMALINK_IN_TEXT.findall(text))


def _classify_link(text: str):
    if permalink_key(text):
        return "permalink", text
    if _URL_ONLY.match(text):
        return "url", text
    return None, ""


def _is_channel(chat: dict, channel: str) -> bool:
    if channel.startswith("@"):
        return (chat.get("username") or "").lower() == channel[1:].lower()
    return str(chat.get("id")) == channel
