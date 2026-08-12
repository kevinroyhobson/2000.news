"""Turn a Telegram reaction update into a curation grade.

Pure functions, no AWS or network — this is the part worth unit-testing.

Two update flavours arrive, and which one you get depends on the chat:

  message_reaction_count  Channels (and other anonymous-reaction chats) send
                          aggregate counts with no user attached. This is what
                          @twothousanddotnews produces, so grading there is
                          anonymous: any subscriber's tap counts. Set
                          TELEGRAM_REACTION_USER_IDS to refuse anonymous
                          updates and only accept identified reactors.
  message_reaction        Groups and private chats send the individual
                          before/after reaction lists, with the user.

Both collapse to the same question: which grade, if any, does the current set
of reactions on this message mean?
"""

import json
import os
from typing import NamedTuple

# Telegram only allows reactions from a fixed emoji set (no ⭐, hence 🏆), and
# custom/premium emoji arrive as a different reaction type we ignore.
DEFAULT_EMOJI_GRADES = {
    # Outstanding: becomes a Tournament exemplar, and gets a written rationale.
    "🏆": "outstanding",
    "❤": "outstanding",
    "💯": "outstanding",
    "🔥": "outstanding",
    # Solid: good, keeps serving on the site, not an exemplar.
    "😁": "solid",
    "🤣": "solid",
    "👍": "solid",
    "👏": "solid",
    # Meh / bad: Get filters both out, so the headline stops being served.
    "😐": "meh",
    "🤨": "meh",
    "👎": "bad",
    "💩": "bad",
    "🥱": "bad",
}

# Ties on reaction count break toward the strongest opinion.
GRADE_PRECEDENCE = ("outstanding", "bad", "solid", "meh")

VARIATION_SELECTOR = "\ufe0f"  # U+FE0F, invisible


class Reaction(NamedTuple):
    """A resolved reaction update. grade None means "ungrade this post"."""

    chat_id: int
    message_id: int
    grade: str


def normalize_emoji(emoji: str) -> str:
    """Clients disagree about the trailing variation selector (❤ vs ❤️)."""
    return (emoji or "").replace(VARIATION_SELECTOR, "")


def load_emoji_grades() -> dict:
    """Emoji -> grade, overridable wholesale via TELEGRAM_EMOJI_GRADES (JSON)."""
    raw = os.environ.get("TELEGRAM_EMOJI_GRADES", "").strip()
    mapping = DEFAULT_EMOJI_GRADES
    if raw:
        try:
            mapping = json.loads(raw)
        except ValueError as e:
            print(f"[reaction] TELEGRAM_EMOJI_GRADES is not valid JSON ({e}); using defaults.")
    return {normalize_emoji(k): v for k, v in mapping.items()}


def load_allowed_user_ids() -> set:
    """Reactor allowlist. Empty (the default) accepts anonymous channel taps."""
    raw = os.environ.get("TELEGRAM_REACTION_USER_IDS", "")
    return {int(part) for part in raw.replace(",", " ").split() if part.strip()}


def _emoji_counts(reactions: list) -> dict:
    """ReactionCount[] -> {emoji: total_count}, skipping non-emoji types."""
    counts = {}
    for entry in reactions or []:
        kind = entry.get("type") or {}
        if kind.get("type") != "emoji":
            continue
        emoji = normalize_emoji(kind.get("emoji"))
        if emoji:
            counts[emoji] = counts.get(emoji, 0) + int(entry.get("total_count", 1))
    return counts


def _emoji_list(reactions: list) -> dict:
    """ReactionType[] (one per reaction, no counts) -> {emoji: 1}."""
    return _emoji_counts([{"type": r, "total_count": 1} for r in reactions or []])


def _winning_grade(counts: dict, emoji_grades: dict):
    """Highest-count mapped emoji wins; ties go by GRADE_PRECEDENCE."""
    graded = [
        (count, emoji_grades[emoji])
        for emoji, count in counts.items()
        if emoji in emoji_grades
    ]
    if not graded:
        return None
    return max(graded, key=lambda pair: (pair[0], -_precedence(pair[1])))[1]


def _precedence(grade: str) -> int:
    return GRADE_PRECEDENCE.index(grade) if grade in GRADE_PRECEDENCE else len(GRADE_PRECEDENCE)


def parse(update: dict, emoji_grades: dict = None, allowed_user_ids: set = None):
    """Resolve an update to a Reaction, or None if it asks for nothing.

    A Reaction with grade=None means every grading emoji was taken off the
    post — an undo of whatever the reactions previously set.
    """
    emoji_grades = emoji_grades if emoji_grades is not None else load_emoji_grades()
    allowed_user_ids = allowed_user_ids if allowed_user_ids is not None else load_allowed_user_ids()

    if "message_reaction_count" in update:
        payload = update["message_reaction_count"]
        if allowed_user_ids:
            # Aggregate counts carry no user, so there is nobody to check
            # against the allowlist.
            print("[reaction] ignoring anonymous reaction: an allowlist is configured.")
            return None
        grade = _winning_grade(_emoji_counts(payload.get("reactions")), emoji_grades)
        return _build(payload, grade)

    if "message_reaction" in update:
        payload = update["message_reaction"]
        user_id = (payload.get("user") or {}).get("id")
        if allowed_user_ids and user_id not in allowed_user_ids:
            print(f"[reaction] ignoring reaction from user {user_id!r}: not in the allowlist.")
            return None
        grade = _winning_grade(_emoji_list(payload.get("new_reaction")), emoji_grades)
        if grade is None:
            # Only treat this as an undo if this user's reaction is what went
            # away — an unmapped emoji landing on the post is not a signal.
            had_grade = _winning_grade(_emoji_list(payload.get("old_reaction")), emoji_grades)
            if had_grade is None:
                return None
        return _build(payload, grade)

    return None


def _build(payload: dict, grade):
    chat_id = (payload.get("chat") or {}).get("id")
    message_id = payload.get("message_id")
    if chat_id is None or message_id is None:
        return None
    return Reaction(chat_id=int(chat_id), message_id=int(message_id), grade=grade)
