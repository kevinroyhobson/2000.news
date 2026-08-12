"""Minimal Telegram Bot API client.

Shared by the hourly headline poster (TelegramAlert) and the reaction grader
(TelegramReaction). stdlib-only so it stays cheap to import in a Lambda whose
whole job is to answer a webhook in a few milliseconds.
"""

import json
import urllib.error
import urllib.request

from lib.ssm_secrets import get_secret

API_ROOT = "https://api.telegram.org"


class TelegramError(RuntimeError):
    """A Bot API call failed, or came back ok=false."""


def call(method: str, payload: dict, timeout: int = 10):
    """POST to the Bot API and return the `result` field."""
    token = get_secret("telegram-bot-token")
    request = urllib.request.Request(
        f"{API_ROOT}/bot{token}/{method}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")[:300]
        raise TelegramError(f"{method} HTTP {e.code}: {detail}") from e
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
        raise TelegramError(f"{method} failed ({type(e).__name__}: {e})") from e
    if not body.get("ok"):
        raise TelegramError(f"{method} returned ok=false: {body.get('description')!r}")
    return body.get("result")


def send_message(chat_id, text: str, reply_to_message_id: int = None, timeout: int = 10) -> dict:
    """Send an HTML message, optionally as a reply. Returns the sent Message."""
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_to_message_id is not None:
        payload["reply_parameters"] = {
            "message_id": reply_to_message_id,
            # A reply to a deleted post should still post, not fail the call.
            "allow_sending_without_reply": True,
        }
    return call("sendMessage", payload, timeout=timeout)
