"""Hourly Telegram alert: post the day's top not-yet-posted headline.

Each run pulls https://api.2000.news/today?seen=<already-posted ids>. The get
API returns Stories[0] = the highest-ranked headline not in `seen` (its
seen_as_top logic), i.e. the best one we haven't posted. We format it, post to
the channel, and record the id in TelegramSentHeadlines.

That record does double duty. Its ids form the `seen` list (bounded to the
last SEEN_WINDOW_SECONDS, matching /today's 3-day pool) and its MessageId is
how TelegramReaction maps an emoji tapped in the channel back to a headline —
so rows outlive the seen window and are kept until the TTL, letting you grade
a post you scroll back to days later.

Best-effort: an API or Telegram hiccup logs and exits without raising, so a
failed hour simply retries on the next.
"""

import html
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

import boto3

from lib.telegram import TelegramError, send_message

API_BASE = os.environ.get("API_BASE", "https://api.2000.news")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SENT_TABLE = os.environ.get("SENT_TABLE", "TelegramSentHeadlines")
# Rows live long enough to resolve a late reaction; only the most recent ones
# count as "seen" for headline selection.
TTL_SECONDS = 14 * 24 * 60 * 60
SEEN_WINDOW_SECONDS = 3 * 24 * 60 * 60

_dynamo = boto3.resource("dynamodb")
_sent_table = _dynamo.Table(SENT_TABLE)


def handler(event, context):
    if not TELEGRAM_CHAT_ID:
        print("TELEGRAM_CHAT_ID not set; cannot post.")
        return
    sent_ids = _load_sent_ids()
    story = _fetch_top_unsent(sent_ids)
    if not story:
        print("No story returned from API; nothing to post.")
        return
    headline_id = story.get("HeadlineId")
    if not headline_id or headline_id in sent_ids:
        # All current top headlines already posted -> API fell back to a seen
        # one. Nothing new this hour.
        print(f"Top story {headline_id!r} already posted or missing id; skipping.")
        return
    try:
        message = send_message(TELEGRAM_CHAT_ID, _format_message(story))
    except TelegramError as e:
        print(f"Telegram send failed for {headline_id} ({e}); will retry next hour.")
        return
    _mark_sent(story, message)
    print(f"Posted headline {headline_id} as message {message.get('message_id')}.")


def _load_sent_ids() -> set:
    """Headline ids posted inside the seen window. Stays small."""
    ids = set()
    cutoff = int(time.time()) - SEEN_WINDOW_SECONDS
    kwargs = {"ProjectionExpression": "HeadlineId, SentAt"}
    while True:
        resp = _sent_table.scan(**kwargs)
        for item in resp.get("Items", []):
            # Rows written before SentAt existed expire on their own; treat
            # them as recent so they aren't re-posted in the meantime.
            if int(item.get("SentAt", cutoff)) >= cutoff:
                ids.add(item["HeadlineId"])
        key = resp.get("LastEvaluatedKey")
        if not key:
            return ids
        kwargs["ExclusiveStartKey"] = key


def _fetch_top_unsent(sent_ids: set):
    """GET /today?seen=... and return Stories[0] (highest-ranked unsent) or None."""
    url = f"{API_BASE}/today"
    if sent_ids:
        url += "?" + urllib.parse.urlencode({"seen": ",".join(sorted(sent_ids))})
    req = urllib.request.Request(url, headers={"User-Agent": "2000news-telegram-alert"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
        print(f"API fetch failed ({type(e).__name__}: {e}).")
        return None
    stories = data.get("Stories") or []
    return stories[0] if stories else None


def _format_message(story: dict) -> str:
    """Message is sent with parse_mode=HTML, so dynamic text gets html-escaped."""
    headline = html.escape(story.get("Headline", "").strip())
    permalink = f"https://www.2000.news/{story['YearMonthDay']}/{story['HeadlineId']}"
    message = f"{headline}\n\n{permalink}"
    original = html.escape(story.get("OriginalHeadline", "").strip())
    if original:
        url = story.get("Url", "").strip()
        if url:
            original = f'<a href="{html.escape(url, quote=True)}">{original}</a>'
        source = html.escape(story.get("Source", "").strip())
        message += f"\n\n({original}, {source})" if source else f"\n\n({original})"
    return message


def _mark_sent(story: dict, message: dict) -> None:
    """Record the post. MessageId/ChatId are the reaction grader's lookup key."""
    item = {
        "HeadlineId": story["HeadlineId"],
        "YearMonthDay": story.get("YearMonthDay", ""),
        "Headline": story.get("Headline", ""),
        "OriginalHeadline": story.get("OriginalHeadline", ""),
        "SentAt": int(time.time()),
        "ExpiresAt": int(time.time()) + TTL_SECONDS,
    }
    message_id = message.get("message_id")
    chat_id = (message.get("chat") or {}).get("id")
    if message_id is not None and chat_id is not None:
        item["MessageId"] = int(message_id)
        item["ChatId"] = int(chat_id)
    _sent_table.put_item(Item=item)
