"""Turn a URL into a story the pipeline can subvert.

nytimes.com links go through the Article Search API (lib/nyt_client), since
the pages themselves serve a paywall stub. Everything else is fetched and
read: the image, title and dates come from the page's meta tags, and a model
picks the article's opening paragraphs out of the visible text, which is the
part nav menus, cookie banners and related-story links make unreliable to
regex.
"""

import datetime
import os
import urllib.error
import urllib.request
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests

from lib import nyt_client
from lib.anthropic_batches import extract_text
from lib.llm_json import parse_json_response
from Subvert.pipeline import get_anthropic_client
from TelegramScoop import page

EXTRACT_MODEL = os.getenv("SCOOP_EXTRACT_MODEL", "claude-sonnet-5")
USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/128.0 Safari/537.36")
MAX_PAGE_BYTES = 2_000_000

EXTRACT_SYSTEM = """You are given the visible text of a news article's web page, navigation and all. Find the article and return JSON only, no prose:
{"headline": "...", "lede": "...", "author": "..." or null, "published_at": "ISO 8601 timestamp" or null}

- lede: the article's opening two to four paragraphs, verbatim, separated by blank lines, stopping around 1200 characters. Leave out captions, bylines, datelines, subscription pitches, cookie notices and related-story links.
- If the page holds no article text (a paywall stub, an error page, a video page), return an empty lede."""


class ArticleError(Exception):
    """Why a URL couldn't become a story, worded for the channel."""


def fetch_story(url: str) -> dict:
    """A story dict in the newsdata.io shape StoriesRepository saves."""
    if nyt_client.is_nyt_url(url):
        story = _nyt_story(url)
        if story:
            return story
    return _story_from_page(url, _download(url))


def _nyt_story(url: str):
    try:
        return nyt_client.fetch_story(url)
    except requests.RequestException as e:
        print(f"NYT lookup failed ({e}); reading the page instead.")
        return None


def _download(url: str) -> str:
    request = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read(MAX_PAGE_BYTES).decode(charset, errors="replace")
    except urllib.error.HTTPError as e:
        raise ArticleError(f"the site answered HTTP {e.code}") from e
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise ArticleError(f"couldn't reach the site ({e})") from e


def _story_from_page(url: str, html: str) -> dict:
    image_url = page.meta(html, "og:image", "og:image:secure_url", "twitter:image")
    if not image_url:
        raise ArticleError("the page has no og:image, and every story needs a picture")

    extracted = _extract_article(page.visible_text(html))
    title = page.meta(html, "og:title", "twitter:title") or extracted.get("headline") or page.title(html)
    if not title:
        raise ArticleError("couldn't find a headline on the page")
    lede = (extracted.get("lede") or "").strip() or page.meta(html, "og:description", "description")
    if not lede:
        raise ArticleError("couldn't find any article text on the page (paywall?)")

    author = page.meta(html, "author", "article:author") or extracted.get("author")
    published = page.meta(html, "article:published_time", "og:updated_time") or extracted.get("published_at")
    return {
        "title": title,
        "link": url,
        "description": lede,
        "pubDate": _iso_timestamp(published),
        "creator": [author] if author else None,
        "content": None,
        "image_url": image_url,
        "video_url": None,
        "language": "english",
        "country": None,
        "keywords": None,
        "category": None,
        "source_id": (urlparse(url).hostname or "").removeprefix("www."),
    }


def _extract_article(text: str) -> dict:
    if not text.strip():
        return {}
    message = get_anthropic_client().messages.create(
        model=EXTRACT_MODEL,
        max_tokens=4096,
        thinking={"type": "adaptive"},
        output_config={"effort": "low"},
        system=EXTRACT_SYSTEM,
        messages=[{"role": "user", "content": text}],
    )
    extracted = parse_json_response(extract_text(message))
    return extracted if isinstance(extracted, dict) else {}


def _iso_timestamp(published) -> str:
    if published:
        try:
            return datetime.datetime.fromisoformat(published).isoformat()
        except ValueError:
            pass
    return datetime.datetime.now(ZoneInfo("America/New_York")).isoformat()
