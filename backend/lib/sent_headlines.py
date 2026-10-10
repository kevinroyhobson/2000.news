"""Records of headlines posted to the Telegram channel (TelegramSentHeadlines).

A row's MessageId is how the reaction grader maps an emoji tapped in the
channel back to a headline, and its SentAt keeps the hourly poster from
posting the same headline again. RationaleMessageId is the bot's latest
rationale reply under the post, which is how a reply to that rationale maps
back to its headline.
"""

import time

from boto3.dynamodb.conditions import Key

# Rows live long enough to resolve a late reaction on a post you scroll back to.
TTL_SECONDS = 14 * 24 * 60 * 60

MESSAGE_INDEX = "MessageIdIndex"
RATIONALE_INDEX = "RationaleMessageIdIndex"


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


def record_rationale_reply(table, headline_id: str, message: dict) -> None:
    table.update_item(
        Key={"HeadlineId": headline_id},
        UpdateExpression="SET RationaleMessageId = :m",
        ExpressionAttributeValues={":m": int(message["message_id"])},
    )


def find_post(table, chat_id: int, message_id: int):
    """The row for the headline posted as this message, if any."""
    return _find(table, MESSAGE_INDEX, "MessageId", chat_id, message_id)


def find_by_rationale_reply(table, chat_id: int, message_id: int):
    """The row for the headline whose latest rationale reply is this message, if any."""
    return _find(table, RATIONALE_INDEX, "RationaleMessageId", chat_id, message_id)


def _find(table, index: str, attribute: str, chat_id: int, message_id: int):
    resp = table.query(IndexName=index, KeyConditionExpression=Key(attribute).eq(message_id))
    for item in resp.get("Items", []):
        if int(item.get("ChatId", 0)) == chat_id:
            return item
    return None
