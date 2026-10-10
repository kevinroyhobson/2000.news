"""Rewrite an outstanding headline's rationale from a reply in the channel.

Invoked asynchronously by webhook.py for replies posted in the channel. A
reply to the bot's rationale, or to the headline post itself, is a note on
what makes the headline land ("also, it's a Dr. Seuss reference"). Claude
rewrites the rationale around that note, the rewrite replaces the headline's
rationale (and so the judge's exemplar for it), and the bot replies with it.
Replying to that rewrite refines it again.

Replies to anything else are ignored.
"""

import os

import boto3

from lib.curation import (
    TABLE_NAME,
    rebuild_exemplar_cache,
    refine_rationale,
    set_rationale,
)
from lib.sent_headlines import find_by_rationale_reply, find_post, record_rationale_reply
from lib.telegram import send_message
from TelegramReaction.grade import grade_reply
from TelegramScoop import commands

CHANNEL = os.environ["TELEGRAM_CHAT_ID"]
SENT_TABLE = os.environ.get("SENT_TABLE", "TelegramSentHeadlines")

_dynamo = boto3.resource("dynamodb")
_sent_table = _dynamo.Table(SENT_TABLE)
_headlines_table = _dynamo.Table(TABLE_NAME)


def handler(event, context):
    request = commands.parse(event["update"], CHANNEL)
    if not request or request.kind != "reply":
        return

    sent = _sent_headline_replied_to(request)
    if not sent:
        print(f"Message {request.message_id} doesn't reply to a headline or its "
              f"rationale; ignoring.")
        return

    year_month_day = sent.get("YearMonthDay", "")
    headline_id = sent["HeadlineId"]
    headline = _headlines_table.get_item(
        Key={"YearMonthDay": year_month_day, "HeadlineId": headline_id}
    ).get("Item") or {}
    if headline.get("Grade") != "outstanding":
        send_message(request.chat_id, "Only outstanding picks have a rationale to rewrite.",
                     reply_to_message_id=request.message_id)
        return

    rationale = refine_rationale(
        headline.get("Headline", ""), headline.get("OriginalHeadline", ""),
        headline.get("Rationale", ""), request.text,
        on_refusal=lambda e: print(f"[refine] {e} — falling back."),
    )
    if not set_rationale(_headlines_table, year_month_day, headline_id, rationale):
        print(f"Not rewriting {headline_id}'s rationale: it's no longer outstanding.")
        return
    print(f"Rewrote {headline_id}'s rationale from message {request.message_id}.")

    message = send_message(request.chat_id, grade_reply("outstanding", rationale),
                           reply_to_message_id=request.message_id)
    record_rationale_reply(_sent_table, headline_id, message)

    count = rebuild_exemplar_cache(_headlines_table)
    print(f"Exemplar cache refreshed ({count} entries).")


def _sent_headline_replied_to(request: commands.Request):
    return (find_by_rationale_reply(_sent_table, request.chat_id, request.reply_to_message_id)
            or find_post(_sent_table, request.chat_id, request.reply_to_message_id))
