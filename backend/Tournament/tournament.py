"""
SubvertedHeadlines-stream trigger for the Tournament pipeline.

Thin starter: acquires the per-day tournament lock, then starts one
TournamentPipeline Step Functions execution. All ranking happens inside the
state machine through the Anthropic Batch API. When a tournament for the day
is already in flight, this exits — the running execution re-checks for new
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

    if not tournament_lock.acquire(day):
        print(f"Tournament already running for {day} — the in-flight execution "
              f"re-checks for new headlines before finishing.")
        return f"Tournament already running for {day}"

    try:
        execution = _sfn.start_execution(
            stateMachineArn=os.environ["TOURNAMENT_STATE_MACHINE_ARN"],
            name=f"{day}-{uuid.uuid4().hex[:8]}",
            input=json.dumps({"day": day, "mode": "same_day"}),
        )
    except Exception:
        tournament_lock.release(day)
        raise

    print(f"Started {execution['executionArn']} for {day}")
    return f"Started tournament pipeline for {day}"
