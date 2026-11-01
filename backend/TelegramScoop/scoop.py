"""Fetch, subvert and rank a story someone posted to the channel, then reply.

Invoked asynchronously by TelegramWebhook. A bare URL becomes one story; a
/scoop request goes to the scoop editor (TelegramScoop/editor.py), which
searches the news and picks up to a few. Either way the stories are
subverted synchronously (Subvert/on_demand), ranked among themselves
(mini_tournament), and the post gets a reply naming the best headline. The headlines wait for the next scheduled tournament to
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
from Subvert.on_demand import subvert_synchronously
from Subvert.pipeline import get_anthropic_client
from Subvert.subvert import do_headlines_exist_for_story, pipeline_story, story_ids_with_headlines
from TelegramScoop import commands, editor
from TelegramScoop.article import ArticleError, fetch_story
from TelegramScoop.mini_tournament import rank

CHANNEL = os.environ["TELEGRAM_CHAT_ID"]
MAX_SEARCH_STORIES = int(os.getenv("SCOOP_MAX_SEARCH_STORIES", "3"))
DAYS_OF_FILED_STORIES = 2
BATCH_PIPELINE_ALLOWANCE = datetime.timedelta(hours=2)
SITE = "https://www.2000.news"
USAGE = ("Post a bare link to a story, or <code>/scoop</code> and what you're after "
         "(<code>/scoop haley stevens endorsing mike rogers</code>) to have the editor find one.")

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
    except (ArticleError, editor.EditorError, ScoopError) as e:
        reply = f"No dice: {html.escape(str(e), quote=False)}."
    except Exception as e:
        traceback.print_exc()
        reply = f"Something broke: {html.escape(f'{type(e).__name__}: {e}', quote=False)}"
    _reply(request, reply)
    langfuse.flush()


def _fulfil(request: commands.Request) -> str:
    if request.kind == "usage":
        return USAGE
    today = _today()
    if request.kind == "url":
        stories = [_save_link(request.text, today)]
    else:
        assignment = _assign(request.text, today)
        stories = _file_picks(assignment.picks, request.text, today)
        if not stories:
            return _nothing_fits(request.text, assignment.close_calls)
    headlines = subvert_synchronously([pipeline_story(story) for story in stories])
    if not headlines:
        raise ScoopError("the writers came back with nothing")
    best = rank(headlines)[0]
    return _report(request, stories, headlines, best)


def _save_link(url: str, today: datetime.date) -> dict:
    story = _save_or_resume(fetch_story(url), f"{ON_DEMAND_FETCH_PREFIX}url", _day_key(today))
    if not story:
        raise ScoopError("that story is already in the paper")
    return story


def _assign(request_text: str, today: datetime.date) -> editor.Assignment:
    unwritten, unavailable = _filed_stories(today)
    return editor.assign(request_text, get_anthropic_client(), NewsdataClient(),
                         unwritten, unavailable, MAX_SEARCH_STORIES, today)


def _filed_stories(today: datetime.date) -> tuple:
    """The stories filed over the last few days that no one has written up,
    and the titles of the rest, each with why it can't run."""
    unwritten_by_title, unavailable = {}, {}
    for days_ago in range(DAYS_OF_FILED_STORIES):
        day = _day_key(today - datetime.timedelta(days=days_ago))
        written_ids = story_ids_with_headlines(day)
        for story in _repo.get_stories_for_day(day):
            if not story.get("StoryId"):
                continue
            if story["StoryId"] in written_ids:
                unavailable[story["Title"]] = "the paper already ran it"
            elif _is_with_the_writers(story):
                unavailable[story["Title"]] = "the writers are on it already"
            elif story.get("ImageUrl"):
                unwritten_by_title.setdefault(story["Title"], story)
    unwritten = [story for title, story in unwritten_by_title.items() if title not in unavailable]
    return unwritten, unavailable


def _is_with_the_writers(story: dict) -> bool:
    """A scheduled story with no headlines yet may still be in the batch
    pipeline, which takes up to an hour."""
    if story.get("FetchCategory", "").startswith(ON_DEMAND_FETCH_PREFIX):
        return False
    if not story.get("RetrievedTime"):
        return False
    filed_at = datetime.datetime.fromisoformat(story["RetrievedTime"])
    return datetime.datetime.now() - filed_at < BATCH_PIPELINE_ALLOWANCE


def _file_picks(picks: list, request_text: str, today: datetime.date) -> list:
    fetch_category = f"{ON_DEMAND_FETCH_PREFIX}scoop:{request_text}"
    stories = (_file(pick, fetch_category, _day_key(today)) for pick in picks)
    return [story for story in stories if story]


def _file(pick: editor.Pick, fetch_category: str, today: str):
    """The Stories item the pick's headlines are written for, carrying the
    editor's note. None if the story can't be saved."""
    note = {"EditorNote": pick.note} if pick.note else {}
    filed = pick.candidate.filed
    if not filed:
        return _repo.save_story(pick.candidate.result, fetch_category, year_month_day=today,
                                extra_attributes=note or None)
    return _claim(filed, {"FetchCategory": fetch_category, **note}, today)


def _claim(filed: dict, attributes: dict, today: str):
    """Moves a filed story into today as an on-demand story, since the
    tournament ranks only today's headlines and the Stories stream leaves
    on-demand stories to the scoop."""
    if filed["YearMonthDay"] != today:
        return _repo.refile_story(filed, today, attributes)
    if not _repo.claim_story(filed, attributes):
        return None
    return {**filed, **attributes}


def _nothing_fits(request_text: str, close_calls: list) -> str:
    reply = f"No dice: nothing out there is really about “{_escape(request_text)}”."
    if not close_calls:
        return reply
    lines = "\n".join(_story_line(c.title, c.url, c.source) for c in close_calls)
    return f"{reply}\n\nClosest:\n{lines}\n\nPost one bare to run it anyway."


def _save_or_resume(story: dict, fetch_category: str, today: str):
    """Save the story, or pick it back up if an earlier attempt at this request
    saved it and then died before writing its headlines. The stream trigger
    skips on-demand stories, so nothing else would ever come back for it."""
    saved = _repo.save_story(story, fetch_category, year_month_day=today)
    if saved:
        return saved
    existing = _repo.get_story(today, story["title"])
    if (existing
            and existing.get("FetchCategory", "").startswith(ON_DEMAND_FETCH_PREFIX)
            and not do_headlines_exist_for_story(today, existing["StoryId"])):
        print(f"Resuming '{existing['Title']}': saved on demand but never subverted.")
        return existing
    return None


def _report(request: commands.Request, stories: list, headlines: list, best: dict) -> str:
    if request.kind == "url":
        lead = f"Wrote {len(headlines)} headlines for “{_escape(stories[0]['Title'])}”."
    else:
        lines = "\n".join(_story_line(s["Title"], s.get("Url"), s.get("Source")) for s in stories)
        lead = (f"Found {_count(len(stories), 'story', 'stories')} for “{_escape(request.text)}”:\n"
                f"{lines}\n\nWrote {len(headlines)} headlines.")
    permalink = f"{SITE}/{best['YearMonthDay']}/{best['HeadlineId']}"
    return (f"{lead}\n\nBest: <b>{_escape(best['Headline'])}</b>\n{permalink}"
            f"\n\nThey join the rankings at the next tournament.")


def _story_line(title: str, url: str, source: str) -> str:
    linked = f'<a href="{html.escape(url)}">{_escape(title)}</a>' if url else _escape(title)
    return f"• {linked} ({_escape(source or 'unknown')})"


def _count(n: int, singular: str, plural: str) -> str:
    return f"{n} {singular if n == 1 else plural}"


def _today() -> datetime.date:
    return datetime.datetime.now(ZoneInfo("America/New_York")).date()


def _day_key(day: datetime.date) -> str:
    return day.strftime("%Y%m%d")


def _escape(text: str) -> str:
    # parse_mode=HTML on a text body only needs &, < and > escaped.
    return html.escape(text, quote=False)


def _reply(request: commands.Request, text: str) -> None:
    try:
        send_message(request.chat_id, text, reply_to_message_id=request.message_id)
        print(f"Replied to message {request.message_id}: {text}")
    except TelegramError as e:
        print(f"Reply to message {request.message_id} failed ({e}).")
