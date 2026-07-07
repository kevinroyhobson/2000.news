"""
Advisory lock that serializes tournament pipeline executions.

A single global lock rather than one per day: every run's cross-day pass
clears and rewrites CrossDayRank across a 3-day window, so concurrent runs —
even for different days — would interleave destructively on the days their
windows share.

Stored as a META item in SubvertedHeadlines (YearMonthDay='META' keeps it out
of day queries, alongside the outstanding_exemplars item). acquire() returns
an owner token, and refresh/release only act while that token still owns the
lock, so a run that outlives its expiry cannot disturb the lock of the run
that took over. The state machine releases the lock on success and on its
error path; the expiry lets a new run take over after a crash that skipped
both.
"""

import time
import uuid

import boto3
from botocore.exceptions import ClientError

_headlines_table = boto3.resource("dynamodb").Table("SubvertedHeadlines")

_LOCK_KEY = {"YearMonthDay": "META", "HeadlineId": "tournament_lock"}
LOCK_TTL_SECONDS = 12 * 3600


def acquire():
    """Take the lock. Returns an owner token, or None while another run
    holds an unexpired lock."""
    token = uuid.uuid4().hex
    now = int(time.time())
    try:
        _headlines_table.put_item(
            Item={**_LOCK_KEY, "OwnerToken": token,
                  "ExpiresAt": now + LOCK_TTL_SECONDS},
            ConditionExpression="attribute_not_exists(HeadlineId) OR ExpiresAt < :now",
            ExpressionAttributeValues={":now": now},
        )
        return token
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return None
        raise


def refresh(token: str):
    """Extend the lock's expiry. A no-op unless token still owns the lock."""
    try:
        _headlines_table.update_item(
            Key=_LOCK_KEY,
            UpdateExpression="SET ExpiresAt = :expires",
            ConditionExpression="OwnerToken = :token",
            ExpressionAttributeValues={
                ":expires": int(time.time()) + LOCK_TTL_SECONDS,
                ":token": token,
            },
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise


def release(token: str):
    """Release the lock. A no-op unless token still owns the lock."""
    try:
        _headlines_table.delete_item(
            Key=_LOCK_KEY,
            ConditionExpression="OwnerToken = :token",
            ExpressionAttributeValues={":token": token},
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
