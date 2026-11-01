"""Reply to /render with a headline's Instagram post, ready to download.

Invoked asynchronously by webhook.py. The first permalink is the headline; a
second one pins the second story, which is otherwise the day's next headline.
The image goes back as a document, so it arrives at full size, captioned
with the permalinks it was made from. Replying "rerender" to it reads them
back and rolls the paper name, photo column and filler again.

Instagram/render.py draws it, the same renderer as `news insta`.
"""

import html
import os
import traceback

from Instagram import render as instagram
from lib.telegram import send_document, send_message
from TelegramScoop import commands

CHANNEL = os.environ["TELEGRAM_CHAT_ID"]
USAGE = ("Send /render with a 2000.news permalink, plus a second one to pin the second story. "
         "Reply rerender to an image to roll it again.")


def handler(event, context):
    request = commands.parse(event["update"], CHANNEL)
    if not request or request.kind != "render":
        return
    permalinks = request.text.split()
    if not 1 <= len(permalinks) <= 2:
        _reply(request, USAGE)
        return

    try:
        paper = instagram.fetch_post_paper(*map(commands.permalink_key, permalinks))
        jpeg = instagram.render_post(paper)
    except instagram.RenderError as e:
        _reply(request, f"No dice: {html.escape(str(e), quote=False)}.")
        return
    except Exception as e:
        traceback.print_exc()
        _reply(request, f"Render broke ({html.escape(type(e).__name__)}). Reply rerender to try again.")
        raise

    headline = paper["Stories"][0]
    send_document(request.chat_id, f"{headline['YearMonthDay']}-{headline['HeadlineId']}.jpg", jpeg,
                  caption=caption(instagram.story_title(headline), permalinks),
                  reply_to_message_id=request.message_id)
    print(f"Rendered {' '.join(permalinks)} for message {request.message_id}.")


def caption(title: str, permalinks: list) -> str:
    """The headline, then the permalinks a rerender reads back."""
    return "\n".join([html.escape(title, quote=False), *permalinks])


def _reply(request: commands.Request, text: str):
    send_message(request.chat_id, text, reply_to_message_id=request.message_id)
