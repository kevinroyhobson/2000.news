"""
SubvertedHeadlines-stream trigger for the Tournament pipeline.

Thin starter: acquires the tournament lock, then starts one
TournamentPipeline Step Functions execution. All ranking happens inside the
state machine through the Anthropic Batch API. When a tournament is already
in flight, this exits — the running execution re-checks for unprocessed
headlines before releasing the lock (see pipeline.finalize).

The stream records are only a signal that new headlines exist; the pipeline
queries the day's table state. The one thing read off them is the Deferred
flag: headlines written on demand (TelegramScoop) wait for the run the next
scheduled fetch sets off, so a batch made up entirely of those starts nothing.
"""

import datetime
import json
import os
import uuid
from zoneinfo import ZoneInfo

import boto3

from lib import tournament_lock

_sfn = boto3.client("stepfunctions")


def tournament(event, context):
    """Start a tournament pipeline execution for the day (or today)."""
    records = (event or {}).get("Records") or []
    if records and all(_is_deferred(record) for record in records):
        print(f"All {len(records)} new headlines are deferred; leaving them for the next run.")
        return "Deferred"

    day = (event or {}).get("date") or \
        datetime.datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")

    lock_token = tournament_lock.acquire()
    if lock_token is None:
        print("Tournament already running — the in-flight execution re-checks "
              "for unprocessed headlines before finishing.")
        return "Tournament already running"

    try:
        execution = _sfn.start_execution(
            stateMachineArn=os.environ["TOURNAMENT_STATE_MACHINE_ARN"],
            name=f"{day}-{uuid.uuid4().hex[:8]}",
            input=json.dumps({"day": day, "mode": "same_day",
                              "lock_token": lock_token}),
        )
    except Exception:
        tournament_lock.release(lock_token)
        raise

    print(f"Started {execution['executionArn']} for {day}")
    return f"Started tournament pipeline for {day}"


def _is_deferred(record: dict) -> bool:
    image = record.get("dynamodb", {}).get("NewImage") or {}
    return image.get("Deferred", {}).get("BOOL") is True
