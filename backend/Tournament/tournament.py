"""
SubvertedHeadlines-stream trigger for the Tournament pipeline.

Thin starter: acquires the tournament lock, then starts one
TournamentPipeline Step Functions execution. All ranking happens inside the
state machine through the Anthropic Batch API. When a tournament is already
in flight, this exits — the running execution re-checks for unprocessed
headlines before releasing the lock (see pipeline.finalize).

The stream records themselves are ignored: the event is only a signal that
new headlines exist, and the pipeline queries the day's table state.
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
