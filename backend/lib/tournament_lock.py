"""
Per-day advisory lock that serializes tournament pipeline executions.

Stored as a META item in SubvertedHeadlines (YearMonthDay='META' keeps it out
of day queries, alongside the outstanding_exemplars item). The state machine
releases the lock on success and on its error path; the expiry lets a new
run take over after a crash that skipped both.
"""

import time

import boto3
from botocore.exceptions import ClientError

_headlines_table = boto3.resource("dynamodb").Table("SubvertedHeadlines")

LOCK_TTL_SECONDS = 12 * 3600


def _lock_key(day: str) -> dict:
    return {"YearMonthDay": "META", "HeadlineId": f"tournament_lock#{day}"}


def acquire(day: str) -> bool:
    """Take the day's lock. Returns False if a live (unexpired) run holds it."""
    now = int(time.time())
    try:
        _headlines_table.put_item(
            Item={**_lock_key(day), "ExpiresAt": now + LOCK_TTL_SECONDS},
            ConditionExpression="attribute_not_exists(HeadlineId) OR ExpiresAt < :now",
            ExpressionAttributeValues={":now": now},
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def refresh(day: str):
    """Extend the lock (used when a finishing execution restarts itself)."""
    _headlines_table.put_item(
        Item={**_lock_key(day), "ExpiresAt": int(time.time()) + LOCK_TTL_SECONDS}
    )


def release(day: str):
    _headlines_table.delete_item(Key=_lock_key(day))
