"""Hourly Telegram alert: post the day's top not-yet-posted headline.

Each run pulls https://api.2000.news/today?seen=<already-posted ids>. The get
API returns Stories[0] = the highest-ranked headline not in `seen` (its
seen_as_top logic), i.e. the best one we haven't posted. We format it, post to
the channel, and record the id in TelegramSentHeadlines with a 3-day TTL so it
isn't repeated and the seen list stays bounded (matching /today's 3-day pool).

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

from lib.ssm_secrets import get_secret

API_BASE = os.environ.get("API_BASE", "https://api.2000.news")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SENT_TABLE = os.environ.get("SENT_TABLE", "TelegramSentHeadlines")
TTL_SECONDS = 3 * 24 * 60 * 60  # 3 days

_dynamo = boto3.resource("dynamodb")
_sent_table = _dynamo.Table(SENT_TABLE)


def handler(event, context):
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
    if _send_telegram(_format_message(story)):
        _mark_sent(headline_id)
        print(f"Posted headline {headline_id}.")
    else:
        print(f"Telegram send failed for {headline_id}; will retry next hour.")


def _load_sent_ids() -> set:
    """Headline ids posted in the last 3 days (TTL purges older). Stays small."""
    ids = set()
    kwargs = {"ProjectionExpression": "HeadlineId"}
    while True:
        resp = _sent_table.scan(**kwargs)
        ids.update(i["HeadlineId"] for i in resp.get("Items", []))
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


def _mark_sent(headline_id: str) -> None:
    _sent_table.put_item(Item={
        "HeadlineId": headline_id,
        "ExpiresAt": int(time.time()) + TTL_SECONDS,
    })


def _send_telegram(text: str) -> bool:
    if not TELEGRAM_CHAT_ID:
        print("TELEGRAM_CHAT_ID not set; cannot post.")
        return False
    token = get_secret("telegram-bot-token")
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            if getattr(resp, "status", 200) >= 400:
                print(f"Telegram sendMessage HTTP {resp.status}.")
                return False
            return True
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"Telegram sendMessage failed ({type(e).__name__}: {e}).")
        return False
