"""What a channel post is asking for, if anything. Pure functions
(tests/test_telegram_commands.py).

Seven requests are recognised, and only from the configured channel. Only its
admins can post there, so only they can make them:

    https://www.2000.news/20261008/ccc10cac   a permalink: post that headline for grading
    https://example.com/some-article          any other bare URL: fetch that story
    /scoop <request>                          have the scoop editor find stories about it
    /scoop <url>                              the same as posting the URL bare
    /render <permalink> [<permalink>]         an Instagram post of the first headline,
                                              with the second as its second story
    /render or "rerender", posted as a reply  render the permalinks in the post it replies to
    /alter <permalink> <instruction>          rewrite that headline the way the instruction asks
    /alter <instruction>, posted as a reply   alter the headline in the post it replies to
    a grading emoji, posted as a reply        grade the post it replies to
    any other text, posted as a reply         a note on the post it replies to

Everything else posted to the channel (the hourly headlines, chat) is ignored.
"""

import re
from dataclasses import dataclass
from urllib.parse import parse_qs, urlsplit

COMMAND = "/scoop"
RENDER_COMMAND = "/render"
ALTER_COMMAND = "/alter"
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
_PERMALINK_IN_TEXT = re.compile(r"https?://(?:www\.)?2000\.news/\d{8}/[0-9a-f]{8}(?![0-9a-z])/?(?:\?\S*)?",
                                re.IGNORECASE)
_COMMAND = re.compile(rf"^{COMMAND}(?:@\w+)?(?:\s+(.*))?$", re.IGNORECASE | re.DOTALL)
_RENDER_COMMAND = re.compile(rf"^{RENDER_COMMAND}(?:@\w+)?(?:\s+(.*))?$", re.IGNORECASE | re.DOTALL)
_ALTER_COMMAND = re.compile(rf"^{ALTER_COMMAND}(?:@\w+)?(?:\s+(.*))?$", re.IGNORECASE | re.DOTALL)
_RERENDER = re.compile(r"^(?:re-?)?render\W*$", re.IGNORECASE)


@dataclass(frozen=True)
class Request:
    kind: str  # "permalink", "url", "search", "render", "alter", "grade", "reply", or "usage" for a bare /scoop
    text: str  # for a grade, the grade the emoji stands for; for a render, its permalinks;
               # for an alter, its permalink then its instruction (see alter_target)
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


def shows_real_headline(url: str) -> bool:
    """Whether a permalink asks for its story's real headline, as one copied in debug mode does."""
    return parse_qs(urlsplit(url).query).get("real") == ["true"]


def alter_target(text: str):
    """The (permalink, instruction) an alter request's text holds. Either is
    empty when the request left it out."""
    first_word, rest = _first_word(text)
    if not permalink_key(first_word):
        return "", text.strip()
    return first_word, rest


def _classify(text: str, replied_to_text: str):
    render = _RENDER_COMMAND.match(text)
    if render:
        return "render", _render_targets(render.group(1) or replied_to_text)
    alter = _ALTER_COMMAND.match(text)
    if alter:
        return "alter", _alter_text((alter.group(1) or "").strip(), replied_to_text)
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
    return " ".join(_canonical_permalink(url) for url in _PERMALINK_IN_TEXT.findall(text))


def _alter_text(argument: str, replied_to_text: str) -> str:
    """The permalink to alter, then the instruction. The permalink is the
    argument's first word, or else the first one in the post replied to."""
    first_word, rest = _first_word(argument)
    if permalink_key(first_word):
        return f"{_satirical_permalink(first_word)} {rest}".strip()
    replied_to_permalinks = _PERMALINK_IN_TEXT.findall(replied_to_text)
    if replied_to_permalinks:
        return f"{_satirical_permalink(replied_to_permalinks[0])} {argument}".strip()
    return argument


def _first_word(text: str):
    words = text.split(None, 1)
    return (words[0], words[1].strip()) if len(words) == 2 else (text.strip(), "")


def _satirical_permalink(url: str) -> str:
    day, headline_id = permalink_key(url)
    return f"https://www.2000.news/{day}/{headline_id}"


def _canonical_permalink(url: str) -> str:
    canonical = _satirical_permalink(url)
    return f"{canonical}?real=true" if shows_real_headline(url) else canonical


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
