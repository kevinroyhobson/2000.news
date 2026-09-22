"""Fetch, subvert and rank a story someone posted to the channel, then reply.

Invoked asynchronously by TelegramWebhook. A bare URL becomes one story; a
/scoop command becomes up to a few from a news search. Either way the
stories are filed under today, subverted synchronously (Subvert/on_demand),
ranked among themselves (mini_tournament), and the post gets a reply naming
the best headline. The headlines wait for the next scheduled tournament to
be ranked for real.

Every outcome is a reply, failures included, and nothing raises out of the
handler: Lambda retries a failed async invocation, and by then the story is
already saved.
"""

import datetime
import html
import os
import traceback
from zoneinfo import ZoneInfo

from langfuse import get_client, observe

from lib.newsdata_client import NewsdataClient
from lib.stories_repository import ON_DEMAND_FETCH_PREFIX, StoriesRepository
from lib.telegram import TelegramError, send_message
from lib.topic_search import save_stories_for_query
from Subvert.on_demand import subvert_synchronously
from Subvert.subvert import pipeline_story
from TelegramScoop import commands
from TelegramScoop.article import ArticleError, fetch_story
from TelegramScoop.mini_tournament import rank

CHANNEL = os.environ["TELEGRAM_CHAT_ID"]
MAX_SEARCH_STORIES = int(os.getenv("SCOOP_MAX_SEARCH_STORIES", "3"))
SITE = "https://www.2000.news"
USAGE = ("Post a bare link to a story, or <code>/scoop &lt;topic&gt;</code> "
         "to search the news for one.")

_repo = StoriesRepository()
langfuse = get_client()


class ScoopError(Exception):
    """Why the request went nowhere, worded for the channel."""


@observe()
def handler(event, context):
    request = commands.parse(event["update"], CHANNEL)
    if not request:
        return
    try:
        reply = _fulfil(request)
    except (ArticleError, ScoopError) as e:
        reply = f"No dice: {html.escape(str(e), quote=False)}."
    except Exception as e:
        traceback.print_exc()
        reply = f"Something broke: {html.escape(f'{type(e).__name__}: {e}', quote=False)}"
    _reply(request, reply)
    langfuse.flush()


def _fulfil(request: commands.Request) -> str:
    if request.kind == "usage":
        return USAGE
    stories = _save_stories(request)
    headlines = subvert_synchronously([pipeline_story(story) for story in stories])
    if not headlines:
        raise ScoopError("the writers came back with nothing")
    best = rank(headlines)[0]
    return _report(request, stories, headlines, best)


def _save_stories(request: commands.Request) -> list:
    today = datetime.datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
    if request.kind == "url":
        story = _repo.save_story(fetch_story(request.text), f"{ON_DEMAND_FETCH_PREFIX}url",
                                 year_month_day=today)
        if not story:
            raise ScoopError("that story is already in the paper")
        return [story]

    stories = save_stories_for_query(
        request.text, _repo, NewsdataClient(), f"{ON_DEMAND_FETCH_PREFIX}scoop:{request.text}",
        max_stories=MAX_SEARCH_STORIES, year_month_day=today,
    )
    if not stories:
        raise ScoopError(f"nothing new out there for “{request.text}”")
    return stories


def _report(request: commands.Request, stories: list, headlines: list, best: dict) -> str:
    if request.kind == "url":
        lead = f"Wrote {len(headlines)} headlines for “{_escape(stories[0]['Title'])}”."
    else:
        lead = (f"Found {_count(len(stories), 'story', 'stories')} for “{_escape(request.text)}” "
                f"and wrote {len(headlines)} headlines.")
    permalink = f"{SITE}/{best['YearMonthDay']}/{best['HeadlineId']}"
    return (f"{lead}\n\nBest: <b>{_escape(best['Headline'])}</b>\n{permalink}"
            f"\n\nThey join the rankings at the next tournament.")


def _count(n: int, singular: str, plural: str) -> str:
    return f"{n} {singular if n == 1 else plural}"


def _escape(text: str) -> str:
    # parse_mode=HTML on a text body only needs &, < and > escaped.
    return html.escape(text, quote=False)


def _reply(request: commands.Request, text: str) -> None:
    try:
        send_message(request.chat_id, text, reply_to_message_id=request.message_id)
    except TelegramError as e:
        print(f"Reply to message {request.message_id} failed ({e}).")
