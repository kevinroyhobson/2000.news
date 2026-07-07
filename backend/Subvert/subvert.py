"""
Stories-stream trigger for the Subvert pipeline.

Thin starter: filters stream records down to stories that still need
headlines, then starts one SubvertPipeline Step Functions execution for the
whole batch. All LLM work happens inside the state machine through the
Anthropic Batch API (brainstorm batch -> generate batch -> save), so nothing
here calls a model.
"""

import json
import os
import uuid

import boto3
from boto3.dynamodb.conditions import Key
from dynamodb_json import json_util as dynamodb_json

_dynamo_resource = boto3.resource("dynamodb")
_headlines_table = _dynamo_resource.Table("SubvertedHeadlines")
_sfn = boto3.client("stepfunctions")


# Generic source-level labels that add no entity signal. Filter them out so
# the brainstorm prompt is dominated by story-specific people/orgs/subjects.
_HINT_NOISE = {
    "top", "politics", "sports", "entertainment", "business",
    "technology", "world", "us", "national",
}


def subvert(event, context):
    """Collect new stories from the stream and hand them to the state machine."""
    stories = []
    seen = set()

    for record in event["Records"]:
        if record["eventName"] not in ("INSERT", "MODIFY"):
            print(
                f"Skipped record {record['eventID']} because it's not an INSERT or MODIFY event."
            )
            continue

        story = dynamodb_json.loads(record["dynamodb"]["NewImage"])
        story_id = story.get("StoryId", "")

        # A story can appear twice in one stream batch (INSERT then MODIFY).
        key = (story["YearMonthDay"], story_id or story["Title"])
        if key in seen:
            continue
        seen.add(key)

        if story_id and do_headlines_exist_for_story(story["YearMonthDay"], story_id):
            print(f"Skipped {story['Title']} because headlines already exist.")
            continue

        stories.append({
            "year_month_day": story["YearMonthDay"],
            "story_id": story_id,
            "title": story["Title"],
            "description": story.get("Description") or "",
            "entity_hints": _collect_entity_hints(story),
        })

    if not stories:
        print("No stories to process.")
        return "No stories to process"

    execution = _sfn.start_execution(
        stateMachineArn=os.environ["SUBVERT_STATE_MACHINE_ARN"],
        name=f"{stories[0]['year_month_day']}-{len(stories)}stories-{uuid.uuid4().hex[:8]}",
        input=json.dumps({"stories": stories}),
    )
    print(f"Started {execution['executionArn']} for {len(stories)} stories")
    return f"Started pipeline for {len(stories)} stories"


def _collect_entity_hints(story: dict) -> list:
    """Merge NYT <category> tags (stored as Category) with newsdata Keywords
    into a single flat hint list. NYT is rich (people / orgs / subjects),
    newsdata varies, ESPN is empty — fine, random_words still fires."""
    hints = []
    for field in ("Category", "Keywords"):
        val = story.get(field)
        if isinstance(val, list):
            hints.extend(h for h in val if isinstance(h, str) and h)
    # Dedup preserving order and drop generic source labels.
    seen = set()
    out = []
    for h in hints:
        if h.lower() in _HINT_NOISE:
            continue
        key = h.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(h)
    return out


def do_headlines_exist_for_story(year_month_day: str, story_id: str) -> bool:
    """Check if any headlines already exist for this story."""
    response = _headlines_table.query(
        KeyConditionExpression=Key("YearMonthDay").eq(year_month_day),
        FilterExpression="StoryId = :sid",
        ExpressionAttributeValues={":sid": story_id},
        Limit=1,
    )
    return len(response.get("Items", [])) > 0
