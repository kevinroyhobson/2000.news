"""Telegram webhook endpoint: POST https://api.2000.news/telegram/reaction

Telegram expects a fast 200 and retries anything else, so this function only
authenticates the request and hands the update to the grader asynchronously —
writing the grade takes a Claude call and a table scan, far longer than a
webhook should hold open.

Authentication is the secret token Telegram echoes back in a header (set with
setWebhook; see Scratch/telegram_webhook.py). The endpoint is public, so an
update that fails the check is rejected before anything else happens.
"""

import base64
import hmac
import json
import os

import boto3

from lib.ssm_secrets import get_secret

SECRET_NAME = os.environ.get("WEBHOOK_SECRET_NAME", "telegram-webhook-secret")
REACTION_FUNCTION_NAME = os.environ.get("REACTION_FUNCTION_NAME", "")
SECRET_HEADER = "x-telegram-bot-api-secret-token"

# Reaction updates are the only thing this bot listens for. Anything else is
# acknowledged and dropped so Telegram stops redelivering it.
HANDLED_UPDATES = ("message_reaction", "message_reaction_count")

_lambda = boto3.client("lambda")


def handler(event, context):
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    presented = (headers.get(SECRET_HEADER) or "").encode("utf-8", "replace")
    if not hmac.compare_digest(presented, get_secret(SECRET_NAME).encode("utf-8")):
        print("Rejected webhook call: bad or missing secret token.")
        return _response(401)

    try:
        update = json.loads(_body(event) or "{}")
    except ValueError as e:
        print(f"Ignoring unparseable update ({e}).")
        return _response(200)

    if not any(key in update for key in HANDLED_UPDATES):
        print(f"Ignoring update of type(s) {sorted(k for k in update if k != 'update_id')}.")
        return _response(200)

    if not REACTION_FUNCTION_NAME:
        print("REACTION_FUNCTION_NAME not set; dropping update.")
        return _response(200)

    _lambda.invoke(
        FunctionName=REACTION_FUNCTION_NAME,
        InvocationType="Event",
        Payload=json.dumps({"update": update}).encode("utf-8"),
    )
    print(f"Dispatched update {update.get('update_id')} to {REACTION_FUNCTION_NAME}.")
    return _response(200)


def _body(event) -> str:
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        return base64.b64decode(body).decode("utf-8")
    return body


def _response(status: int) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": "{}",
    }
