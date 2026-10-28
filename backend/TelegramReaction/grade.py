"""Grade a headline from an emoji replied to its Telegram post.

Invoked asynchronously by webhook.py when the channel replies to a headline
post, or to the bot's rationale under one, with nothing but a grading emoji.
Only channel admins can post, so only they can grade. Does exactly what
`news curate` does at the keyboard — writes the grade, asks Claude why an
outstanding one works, rebuilds the judge's exemplar cache — and replies with
the grade and, for an outstanding pick, that one-sentence explanation.

Every write carries the Telegram update id and is refused when a later update
has already landed, so overlapping invocations for the same post (grading is
slow) settle on the newest grade.
"""

import html
import os

import boto3

from lib.curation import (
    TABLE_NAME,
    apply_grade,
    claim_update,
    gen_rationale,
    rebuild_exemplar_cache,
)
from lib.sent_headlines import find_replied_to, record_rationale_reply
from lib.telegram import TelegramError, send_message
from TelegramScoop import commands

CHANNEL = os.environ["TELEGRAM_CHAT_ID"]
SENT_TABLE = os.environ.get("SENT_TABLE", "TelegramSentHeadlines")
GRADE_SOURCE = "telegram"

GRADE_REPLY = {
    "outstanding": "🔥 <b>Outstanding</b>",
    "solid": "👍 <b>Solid</b>",
    "meh": "😐 <b>Meh</b>",
    "bad": "👎 <b>Bad</b>",
}

_dynamo = boto3.resource("dynamodb")
_sent_table = _dynamo.Table(SENT_TABLE)
_headlines_table = _dynamo.Table(TABLE_NAME)


def handler(event, context):
    update = event["update"]
    request = commands.parse(update, CHANNEL)
    if not request or request.kind != "grade":
        return

    sent = find_replied_to(_sent_table, request.chat_id, request.reply_to_message_id)
    if not sent:
        print(f"Message {request.message_id} doesn't reply to a headline or its "
              f"rationale; ignoring.")
        return

    update_id = int(update["update_id"])
    item = _load_headline(sent.get("YearMonthDay", ""), sent["HeadlineId"])
    applied_update_id = int(item.get("TelegramUpdateId", -1))

    if applied_update_id > update_id:
        print(f"Update {update_id} on {sent['HeadlineId']} is stale; a newer grade is applied.")
        return
    if applied_update_id == update_id:
        # An earlier attempt at this update wrote the grade but failed before
        # the exemplar cache was rebuilt, and this is the retry.
        _refresh_exemplars()
        return

    _grade(item, sent, request, update_id)


def _grade(item: dict, sent: dict, request: commands.Request, update_id: int) -> None:
    year_month_day = sent.get("YearMonthDay", "")
    headline_id = sent["HeadlineId"]
    grade = request.text
    was_outstanding = item.get("Grade") == "outstanding"

    if item.get("Grade") == grade:
        if not claim_update(_headlines_table, year_month_day, headline_id, update_id):
            print(f"Not acknowledging {headline_id}: a newer grade already applied.")
            return
        print(f"{headline_id} is already {grade}; skipping.")
        _reply(request, f"Already {GRADE_REPLY[grade]}")
        return

    rationale = _rationale(item, sent) if grade == "outstanding" else ""
    written = apply_grade(_headlines_table, year_month_day, headline_id, grade,
                          rationale=rationale, source=GRADE_SOURCE,
                          telegram_update_id=update_id)
    if not written:
        print(f"Not grading {headline_id}: a newer grade already applied.")
        return
    print(f"Graded {headline_id} as {grade} from a Telegram reply.")
    message = _reply(request, grade_reply(grade, rationale))
    if message and rationale:
        record_rationale_reply(_sent_table, headline_id, message)

    # Replies go out before the rebuild: whoever posted the grade is waiting on
    # them, and the rebuild is slow.
    if grade == "outstanding" or was_outstanding:
        _refresh_exemplars()


def _rationale(item: dict, sent: dict) -> str:
    """The same one-sentence explanation the CLI writes for outstanding picks."""
    headline = item.get("Headline") or sent.get("Headline", "")
    original = item.get("OriginalHeadline") or sent.get("OriginalHeadline", "")
    try:
        return gen_rationale(headline, original,
                             on_refusal=lambda e: print(f"[grade] {e} — falling back."))
    except Exception as e:
        # A missing rationale is worth less than a lost grade; save the grade.
        print(f"[grade] rationale generation failed ({type(e).__name__}: {e}).")
        return ""


def _refresh_exemplars() -> None:
    """Raises on failure so the async invocation retries and rebuilds again."""
    count = rebuild_exemplar_cache(_headlines_table)
    print(f"Exemplar cache refreshed ({count} entries).")


def grade_reply(grade: str, rationale: str) -> str:
    text = GRADE_REPLY[grade]
    if rationale:
        # parse_mode=HTML on a text body only needs &, < and > escaped; leaving
        # quotes alone keeps the apostrophes in a rationale readable.
        text += f"\n\n{html.escape(rationale, quote=False)}"
    return text


def _reply(request: commands.Request, text: str):
    """The sent Message, or None if the reply failed."""
    try:
        return send_message(request.chat_id, text, reply_to_message_id=request.message_id)
    except TelegramError as e:
        # The grade is already written; a failed reply isn't worth a retry.
        print(f"Reply to message {request.message_id} failed ({e}).")
        return None


def _load_headline(year_month_day: str, headline_id: str) -> dict:
    resp = _headlines_table.get_item(
        Key={"YearMonthDay": year_month_day, "HeadlineId": headline_id}
    )
    return resp.get("Item") or {}
