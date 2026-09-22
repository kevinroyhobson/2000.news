"""Grade a headline from an emoji reaction on its Telegram post.

Invoked asynchronously by webhook.py. Does exactly what `news curate` does at
the keyboard — writes the grade, asks Claude why an outstanding one works,
rebuilds the judge's exemplar cache — and replies to the post with the grade
and, for an outstanding pick, that one-sentence explanation.

Taking the reaction off the post again undoes the grade and replies to say so,
as long as the grade came from a reaction (a grade set at the CLI is left alone).

Every write carries the Telegram update id and is refused when a later update
has already landed, so overlapping invocations for the same post (grading is
slow, and Telegram sends one update per tap) settle on the newest reaction.
"""

import html
import os

import boto3
from boto3.dynamodb.conditions import Key

from lib.curation import (
    TABLE_NAME,
    apply_grade,
    clear_grade,
    gen_rationale,
    rebuild_exemplar_cache,
)
from lib.telegram import TelegramError, send_message
from TelegramReaction import reactions

SENT_TABLE = os.environ.get("SENT_TABLE", "TelegramSentHeadlines")
MESSAGE_INDEX = "MessageIdIndex"

# Marks grades this handler wrote, so an undo can't wipe a CLI grade.
GRADE_SOURCE = "telegram-reaction"

GRADE_REPLY = {
    "outstanding": "🔥 <b>Outstanding</b>",
    "solid": "👍 <b>Solid</b>",
    "bad": "👎 <b>Bad</b>",
}

_dynamo = boto3.resource("dynamodb")
_sent_table = _dynamo.Table(SENT_TABLE)
_headlines_table = _dynamo.Table(TABLE_NAME)


def handler(event, context):
    update = event["update"]
    reaction = reactions.parse(update)
    if not reaction:
        return

    sent = _lookup_sent(reaction)
    if not sent:
        print(f"No posted headline for message {reaction.message_id} "
              f"in chat {reaction.chat_id}; ignoring.")
        return

    update_id = int(update["update_id"])
    year_month_day = sent.get("YearMonthDay", "")
    headline_id = sent["HeadlineId"]
    item = _load_headline(year_month_day, headline_id)
    applied_update_id = int(item.get("ReactionUpdateId", -1))

    if applied_update_id > update_id:
        print(f"Update {update_id} on {headline_id} is stale; a newer reaction is applied.")
        return
    if applied_update_id == update_id:
        # An earlier attempt at this update wrote the grade but failed before
        # the exemplar cache was rebuilt, and this is the retry.
        _refresh_exemplars()
        return

    _grade(item, sent, reaction, update_id)


def _grade(item: dict, sent: dict, reaction: reactions.Reaction, update_id: int) -> None:
    year_month_day = sent.get("YearMonthDay", "")
    headline_id = sent["HeadlineId"]
    was_outstanding = item.get("Grade") == "outstanding"

    if reaction.grade is None:
        cleared = clear_grade(_headlines_table, year_month_day, headline_id,
                              only_if_source=GRADE_SOURCE, reaction_update_id=update_id)
        if not cleared:
            print(f"Not clearing {headline_id}: not a reaction grade, or a newer "
                  f"reaction already applied.")
            return
        print(f"Cleared reaction grade on {headline_id}.")
        _reply(reaction, "Grade cleared")
    else:
        if item.get("Grade") == reaction.grade and item.get("GradeSource") == GRADE_SOURCE:
            print(f"{headline_id} is already {reaction.grade} from a reaction; skipping.")
            return
        rationale = _rationale(item, sent) if reaction.grade == "outstanding" else ""
        written = apply_grade(_headlines_table, year_month_day, headline_id, reaction.grade,
                              rationale=rationale, source=GRADE_SOURCE,
                              reaction_update_id=update_id)
        if not written:
            print(f"Not grading {headline_id}: a newer reaction already applied.")
            return
        print(f"Graded {headline_id} as {reaction.grade} from a Telegram reaction.")
        _reply(reaction, _grade_reply(reaction.grade, rationale))

    # Replies go out before the rebuild: whoever tapped the emoji is waiting on
    # them, and the rebuild is slow.
    if reaction.grade == "outstanding" or was_outstanding:
        _refresh_exemplars()


def _rationale(item: dict, sent: dict) -> str:
    """The same one-sentence explanation the CLI writes for outstanding picks."""
    headline = item.get("Headline") or sent.get("Headline", "")
    original = item.get("OriginalHeadline") or sent.get("OriginalHeadline", "")
    try:
        return gen_rationale(headline, original,
                             on_refusal=lambda e: print(f"[reaction] {e} — falling back."))
    except Exception as e:
        # A missing rationale is worth less than a lost grade; save the grade.
        print(f"[reaction] rationale generation failed ({type(e).__name__}: {e}).")
        return ""


def _refresh_exemplars() -> None:
    """Raises on failure so the async invocation retries and rebuilds again."""
    count = rebuild_exemplar_cache(_headlines_table)
    print(f"Exemplar cache refreshed ({count} entries).")


def _grade_reply(grade: str, rationale: str) -> str:
    text = GRADE_REPLY[grade]
    if rationale:
        # parse_mode=HTML on a text body only needs &, < and > escaped; leaving
        # quotes alone keeps the apostrophes in a rationale readable.
        text += f"\n\n{html.escape(rationale, quote=False)}"
    return text


def _reply(reaction: reactions.Reaction, text: str) -> None:
    try:
        send_message(reaction.chat_id, text, reply_to_message_id=reaction.message_id)
    except TelegramError as e:
        # The grade is already written; a failed reply isn't worth a retry.
        print(f"Reply to message {reaction.message_id} failed ({e}).")


def _lookup_sent(reaction: reactions.Reaction):
    """Find the posted-headline record behind a reacted-to message."""
    resp = _sent_table.query(
        IndexName=MESSAGE_INDEX,
        KeyConditionExpression=Key("MessageId").eq(reaction.message_id),
    )
    for item in resp.get("Items", []):
        if int(item.get("ChatId", 0)) == reaction.chat_id:
            return item
    return None


def _load_headline(year_month_day: str, headline_id: str) -> dict:
    resp = _headlines_table.get_item(
        Key={"YearMonthDay": year_month_day, "HeadlineId": headline_id}
    )
    return resp.get("Item") or {}
