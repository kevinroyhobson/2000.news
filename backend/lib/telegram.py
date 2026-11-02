"""Minimal Telegram Bot API client.

Shared by the hourly headline poster (TelegramAlert) and the channel workers
(TelegramReaction, TelegramScoop, TelegramRender). stdlib-only, so the webhook
stays cheap to cold-start.
"""

import html
import json
import mimetypes
import urllib.error
import urllib.request
import uuid

from lib.ssm_secrets import get_secret

API_ROOT = "https://api.telegram.org"


class TelegramError(RuntimeError):
    """A Bot API call failed, or came back ok=false."""


def call(method: str, payload: dict, timeout: int = 10):
    """POST to the Bot API and return the `result` field."""
    return _post(method, json.dumps(payload).encode("utf-8"), "application/json", timeout)


def _post(method: str, body: bytes, content_type: str, timeout: int):
    token = get_secret("telegram-bot-token")
    request = urllib.request.Request(
        f"{API_ROOT}/bot{token}/{method}",
        data=body,
        headers={"Content-Type": content_type},
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
        payload["reply_parameters"] = _reply_parameters(reply_to_message_id)
    return call("sendMessage", payload, timeout=timeout)


def send_document(chat_id, file_name: str, content: bytes, caption: str = "",
                  reply_to_message_id: int = None, timeout: int = 30) -> dict:
    """Send a file with an HTML caption, optionally as a reply. Returns the sent
    Message. A document arrives byte for byte; sendPhoto would recompress an
    image and shrink it to fit 1280px."""
    fields = {"chat_id": str(chat_id), "caption": caption, "parse_mode": "HTML"}
    if reply_to_message_id is not None:
        fields["reply_parameters"] = json.dumps(_reply_parameters(reply_to_message_id))
    body, content_type = multipart_form(fields, "document", file_name, content)
    return _post("sendDocument", body, content_type, timeout)


def multipart_form(fields: dict, file_field: str, file_name: str, content: bytes):
    """A multipart/form-data body of text fields and one file, and its Content-Type."""
    boundary = uuid.uuid4().hex
    file_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
    parts = [
        f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'.encode("utf-8")
        for name, value in fields.items()
    ]
    parts.append(
        f'--{boundary}\r\nContent-Disposition: form-data; name="{file_field}"; filename="{file_name}"\r\n'
        f'Content-Type: {file_type}\r\n\r\n'.encode("utf-8") + content + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode("utf-8"))
    return b"".join(parts), f"multipart/form-data; boundary={boundary}"


def _reply_parameters(message_id: int) -> dict:
    # A reply to a deleted post should still post, not fail the call.
    return {"message_id": message_id, "allow_sending_without_reply": True}


def source_lines(sources: list) -> str:
    """One '(Real headline, source)' line per source, each headline linked
    to its article. sources are dicts with Title, Url and Source."""
    return "\n".join(line for line in map(_attribution, sources) if line)


def _attribution(source: dict) -> str:
    original = html.escape((source.get("Title") or "").strip())
    if not original:
        return ""
    url = (source.get("Url") or "").strip()
    if url:
        original = f'<a href="{html.escape(url, quote=True)}">{original}</a>'
    source_id = html.escape((source.get("Source") or "").strip())
    return f"({original}, {source_id})" if source_id else f"({original})"

