"""Grade a headline from an emoji reaction on its Telegram post.

Invoked asynchronously by webhook.py. Does exactly what `news curate` does at
the keyboard — writes the grade, asks Claude why an outstanding one works,
rebuilds the judge's exemplar cache — and then replies in the channel with the
verdict and, for outstanding picks, that one-sentence explanation.

Taking the reaction off the post again undoes the grade, as long as the grade
came from a reaction (a grade set at the CLI is left alone).
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
# all | outstanding | none — which grades get a reply posted in the channel.
REPLY_ON_GRADE = os.environ.get("TELEGRAM_REPLY_ON_GRADE", "all").lower()

# Marks grades this handler wrote, so an undo can't wipe a CLI grade.
GRADE_SOURCE = "telegram-reaction"

GRADE_REPLY = {
    "outstanding": "🏆 <b>Outstanding</b> — saved, and added to the judge's exemplars.",
    "solid": "👍 <b>Solid</b> — saved.",
    "meh": "😐 <b>Meh</b> — saved; pulled from the site.",
    "bad": "👎 <b>Bad</b> — saved; pulled from the site.",
}

_dynamo = boto3.resource("dynamodb")
_sent_table = _dynamo.Table(SENT_TABLE)
_headlines_table = _dynamo.Table(TABLE_NAME)
_conditional_failed = _dynamo.meta.client.exceptions.ConditionalCheckFailedException


def handler(event, context):
    reaction = reactions.parse(event.get("update") or event)
    if not reaction:
        return

    sent = _lookup_sent(reaction)
    if not sent:
        print(f"No posted headline for message {reaction.message_id} "
              f"in chat {reaction.chat_id}; ignoring.")
        return

    headline_id = sent["HeadlineId"]
    previous_claim = sent.get("ReactionGrade")
    if not _claim(headline_id, reaction.grade, previous_claim):
        print(f"Reaction on {headline_id} already applied as {reaction.grade!r}; skipping.")
        return

    try:
        _grade(sent, reaction)
    except Exception:
        _restore_claim(headline_id, previous_claim)
        raise


def _grade(sent: dict, reaction: reactions.Reaction) -> None:
    headline_id = sent["HeadlineId"]
    year_month_day = sent.get("YearMonthDay", "")
    item = _load_headline(year_month_day, headline_id)
    was_outstanding = item.get("Grade") == "outstanding"

    if reaction.grade is None:
        if not clear_grade(_headlines_table, year_month_day, headline_id,
                           only_if_source=GRADE_SOURCE):
            print(f"Not clearing {headline_id}: its grade wasn't set by a reaction.")
            return
        print(f"Cleared reaction grade on {headline_id}.")
        if was_outstanding:
            _refresh_exemplars()
        return

    rationale = ""
    if reaction.grade == "outstanding":
        rationale = _rationale(item, sent)

    apply_grade(_headlines_table, year_month_day, headline_id, reaction.grade,
                rationale=rationale, source=GRADE_SOURCE)
    print(f"Graded {headline_id} as {reaction.grade} from a Telegram reaction.")

    # Reply first: whoever tapped the emoji is waiting on it, and the rebuild
    # below is slow.
    _reply(reaction, reaction.grade, rationale)

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
    try:
        count = rebuild_exemplar_cache(_headlines_table)
        print(f"Exemplar cache refreshed ({count} entries).")
    except Exception as e:
        print(f"Exemplar cache refresh failed ({type(e).__name__}: {e}).")


def _reply(reaction: reactions.Reaction, grade: str, rationale: str) -> None:
    if REPLY_ON_GRADE == "none" or (REPLY_ON_GRADE == "outstanding" and grade != "outstanding"):
        return
    text = GRADE_REPLY[grade]
    if rationale:
        # parse_mode=HTML on a text body only needs &, < and > escaped; leaving
        # quotes alone keeps the apostrophes in a rationale readable.
        text += f"\n\n{html.escape(rationale, quote=False)}"
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


def _claim(headline_id: str, grade, previous) -> bool:
    """Record the grade on the sent row first, so Telegram's redeliveries and
    repeated count updates don't re-grade (or re-reply) for the same state."""
    if grade == previous:
        return False
    try:
        if grade is None:
            _sent_table.update_item(
                Key={"HeadlineId": headline_id},
                UpdateExpression="REMOVE ReactionGrade",
                ConditionExpression="attribute_exists(ReactionGrade)",
            )
        else:
            _sent_table.update_item(
                Key={"HeadlineId": headline_id},
                UpdateExpression="SET ReactionGrade = :g",
                ConditionExpression="attribute_not_exists(ReactionGrade) OR ReactionGrade <> :g",
                ExpressionAttributeValues={":g": grade},
            )
    except _conditional_failed:
        return False
    return True


def _restore_claim(headline_id: str, previous) -> None:
    """Put the claim back after a failure so a retry can pick the work up."""
    try:
        if previous is None:
            _sent_table.update_item(
                Key={"HeadlineId": headline_id},
                UpdateExpression="REMOVE ReactionGrade",
            )
        else:
            _sent_table.update_item(
                Key={"HeadlineId": headline_id},
                UpdateExpression="SET ReactionGrade = :g",
                ExpressionAttributeValues={":g": previous},
            )
    except Exception as e:
        print(f"Could not restore the reaction claim on {headline_id} ({e}).")
