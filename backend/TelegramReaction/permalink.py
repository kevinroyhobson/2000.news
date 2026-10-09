"""Post a headline to the channel on request, so it can be graded by reaction.

Invoked asynchronously by webhook.py when someone posts a bare 2000.news
permalink. The bot replies with that headline and records the reply in
TelegramSentHeadlines exactly as the hourly poster records its posts, which
is what lets grade.py resolve an emoji tapped on the reply.

A headline that is already in the channel isn't posted twice: the reply
points at the original post instead, which keeps its reactions working.
"""

import html
import os

import boto3

from lib.curation import TABLE_NAME
from lib.sent_headlines import record_post
from lib.telegram import send_message
from TelegramScoop import commands

CHANNEL = os.environ["TELEGRAM_CHAT_ID"]
SENT_TABLE = os.environ.get("SENT_TABLE", "TelegramSentHeadlines")

_dynamo = boto3.resource("dynamodb")
_sent_table = _dynamo.Table(SENT_TABLE)
_headlines_table = _dynamo.Table(TABLE_NAME)


def handler(event, context):
    request = commands.parse(event["update"], CHANNEL)
    if not request or request.kind != "permalink":
        return

    year_month_day, headline_id = commands.permalink_key(request.text)
    headline = _headlines_table.get_item(
        Key={"YearMonthDay": year_month_day, "HeadlineId": headline_id}
    ).get("Item")
    if not headline:
        send_message(request.chat_id, "No headline at that link.",
                     reply_to_message_id=request.message_id)
        return

    original_post = _original_post(headline_id, request.chat_id)
    if original_post:
        send_message(request.chat_id, "Already posted. React on this one to grade it.",
                     reply_to_message_id=original_post)
        return

    message = send_message(request.chat_id, format_headline(headline),
                           reply_to_message_id=request.message_id)
    record_post(_sent_table, headline, message)
    print(f"Posted {year_month_day}/{headline_id} for grading as message "
          f"{message.get('message_id')}.")


def format_headline(headline: dict) -> str:
    """The headline, the real one(s) it riffs on, and any grade it already has."""
    text = html.escape(headline.get("Headline", "").strip(), quote=False)
    sources = headline.get("SourceHeadlines") or [headline.get("OriginalHeadline", "")]
    originals = " + ".join(source.strip() for source in sources if source)
    if originals:
        text += f"\n\n({html.escape(originals, quote=False)})"
    if headline.get("Grade"):
        text += f"\n\nCurrently graded <b>{headline['Grade']}</b>."
    return text


def _original_post(headline_id: str, chat_id: int):
    """The message id this headline was already posted as in this chat, if any."""
    sent = _sent_table.get_item(Key={"HeadlineId": headline_id}).get("Item") or {}
    if "MessageId" in sent and int(sent.get("ChatId", 0)) == chat_id:
        return int(sent["MessageId"])
    return None

