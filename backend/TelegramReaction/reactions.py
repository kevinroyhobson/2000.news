"""Turn a Telegram reaction update into a curation grade.

Two update flavours arrive, and which one you get depends on the chat:

  message_reaction_count  Channels send aggregate counts with no user
                          attached, so any subscriber's tap counts.
  message_reaction        Groups and private chats send the individual
                          before/after reaction lists, with the user.

Either way the emoji decide the grade, and the most-tapped grading emoji wins.
"""

from typing import NamedTuple

# Telegram only allows reactions from its own fixed emoji set, which is why
# it's 🤣 and not 😂.
EMOJI_GRADES = {
    "🔥": "outstanding",
    "👍": "solid",
    "🤣": "solid",
    "👎": "bad",
}

# Ties on reaction count break toward the strongest opinion.
GRADE_PRECEDENCE = ("outstanding", "bad", "solid")

VARIATION_SELECTOR = "️"


class Reaction(NamedTuple):
    """A resolved reaction update. grade None means "ungrade this post"."""

    chat_id: int
    message_id: int
    grade: str


def normalize_emoji(emoji: str) -> str:
    """Clients disagree about the trailing variation selector (❤ vs ❤️)."""
    return (emoji or "").replace(VARIATION_SELECTOR, "")


def parse(update: dict):
    """Resolve an update to a Reaction, or None if it asks for nothing.

    A Reaction with grade=None means every grading emoji was taken off the
    post: an undo of whatever the reactions previously set.
    """
    if "message_reaction_count" in update:
        payload = update["message_reaction_count"]
        return _build(payload, _winning_grade(_emoji_counts(payload.get("reactions"))))

    if "message_reaction" in update:
        payload = update["message_reaction"]
        grade = _winning_grade(_emoji_list(payload.get("new_reaction")))
        if grade is None:
            # An unmapped emoji landing on the post is not a signal; only a
            # grading emoji going away is an undo.
            previous = _winning_grade(_emoji_list(payload.get("old_reaction")))
            if previous is None:
                return None
        return _build(payload, grade)

    return None


def _emoji_counts(reactions: list) -> dict:
    """ReactionCount[] -> {emoji: total_count}, skipping custom (premium) emoji."""
    counts = {}
    for entry in reactions or []:
        reaction_type = entry.get("type") or {}
        if reaction_type.get("type") != "emoji":
            continue
        emoji = normalize_emoji(reaction_type.get("emoji"))
        if emoji:
            counts[emoji] = counts.get(emoji, 0) + int(entry.get("total_count", 1))
    return counts


def _emoji_list(reactions: list) -> dict:
    """ReactionType[] (one per reaction, no counts) -> {emoji: 1}."""
    return _emoji_counts([{"type": reaction, "total_count": 1} for reaction in reactions or []])


def _winning_grade(counts: dict):
    """Highest-count grading emoji wins; ties go by GRADE_PRECEDENCE."""
    graded = [
        (count, EMOJI_GRADES[emoji])
        for emoji, count in counts.items()
        if emoji in EMOJI_GRADES
    ]
    if not graded:
        return None
    return max(graded, key=lambda pair: (pair[0], -GRADE_PRECEDENCE.index(pair[1])))[1]


def _build(payload: dict, grade):
    chat_id = (payload.get("chat") or {}).get("id")
    message_id = payload.get("message_id")
    if chat_id is None or message_id is None:
        return None
    return Reaction(chat_id=int(chat_id), message_id=int(message_id), grade=grade)
