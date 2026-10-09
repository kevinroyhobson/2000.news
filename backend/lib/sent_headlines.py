"""Records of headlines posted to the Telegram channel (TelegramSentHeadlines).

A row's MessageId is how the reaction grader maps an emoji tapped in the
channel back to a headline, and its SentAt keeps the hourly poster from
posting the same headline again.
"""

import time

# Rows live long enough to resolve a late reaction on a post you scroll back to.
TTL_SECONDS = 14 * 24 * 60 * 60


def record_post(table, headline: dict, message: dict) -> None:
    now = int(time.time())
    item = {
        "HeadlineId": headline["HeadlineId"],
        "YearMonthDay": headline.get("YearMonthDay", ""),
        "Headline": headline.get("Headline", ""),
        "OriginalHeadline": headline.get("OriginalHeadline", ""),
        "SentAt": now,
        "ExpiresAt": now + TTL_SECONDS,
    }
    message_id = message.get("message_id")
    chat_id = (message.get("chat") or {}).get("id")
    if message_id is not None and chat_id is not None:
        item["MessageId"] = int(message_id)
        item["ChatId"] = int(chat_id)
    table.put_item(Item=item)
