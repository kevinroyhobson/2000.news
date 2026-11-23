"""Rewrite a published headline the way /alter asks.

Invoked asynchronously by webhook.py. Claude edits the headline the way the
instruction asks, the edit replaces the headline's text in place, so its
permalink serves the new wording, and the old wording and instruction are
appended to its Alterations. The bot replies with the new headline and its
permalink, so replying /alter to that reply alters it again.

An outstanding headline is one of the judge's exemplars, so its exemplar
cache is rebuilt to pick up the new wording.
"""

import html
import os
import traceback

import boto3

from lib.curation import TABLE_NAME, alter_headline, rebuild_exemplar_cache, set_headline
from lib.telegram import send_message
from TelegramScoop import commands

CHANNEL = os.environ["TELEGRAM_CHAT_ID"]
USAGE = ("Send /alter with a 2000.news permalink and how to change the headline, "
         "or reply /alter and how to change it to a post with a permalink.")

_headlines_table = boto3.resource("dynamodb").Table(TABLE_NAME)


def handler(event, context):
    request = commands.parse(event["update"], CHANNEL)
    if not request or request.kind != "alter":
        return
    permalink, instruction = commands.alter_target(request.text)
    if not (permalink and instruction):
        _reply(request, USAGE)
        return

    year_month_day, headline_id = commands.permalink_key(permalink)
    headline = _headlines_table.get_item(
        Key={"YearMonthDay": year_month_day, "HeadlineId": headline_id}
    ).get("Item")
    if not headline:
        _reply(request, "No headline at that link.")
        return

    current = headline.get("Headline", "")
    try:
        altered = alter_headline(current, headline.get("OriginalHeadline", ""), instruction,
                                 on_refusal=lambda e: print(f"[alter] {e} — falling back."))
    except Exception as e:
        traceback.print_exc()
        _reply(request, f"Alter broke ({html.escape(type(e).__name__)}). Send it again to retry.")
        raise

    if altered == current:
        _reply(request, f"Left it as is:\n\n<b>{_escape(current)}</b>\n\n{permalink}")
        return
    if not set_headline(_headlines_table, year_month_day, headline_id, current, altered, instruction):
        _reply(request, "That headline changed while I was working on it. Send it again to retry.")
        return
    print(f"Altered {year_month_day}/{headline_id} from message {request.message_id}: "
          f"{current!r} -> {altered!r}")

    _reply(request, altered_reply(current, altered, permalink))
    if headline.get("Grade") == "outstanding":
        count = rebuild_exemplar_cache(_headlines_table)
        print(f"Exemplar cache refreshed ({count} entries).")


def altered_reply(current: str, altered: str, permalink: str) -> str:
    return f"<b>{_escape(altered)}</b>\n\nWas: {_escape(current)}\n\n{permalink}"


def _escape(text: str) -> str:
    return html.escape(text, quote=False)


def _reply(request: commands.Request, text: str):
    send_message(request.chat_id, text, reply_to_message_id=request.message_id)
