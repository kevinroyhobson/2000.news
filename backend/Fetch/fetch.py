"""
Scheduled Lambda that fetches news stories from a mix of sources.

Runs 4x/day. Each run fetches 22 stories total, distributed across 10 sources:
  - 1 advice  (newsdata.io targeted query for syndicated advice columns)
  - 2 newsdata entertainment
  - 2 newsdata wildcard
  - 3 ESPN top stories
  - 3 NYT MostViewed
  - 3 NYT HomePage
  - 2 NYT Technology
  - 2 NYT Business
  - 2 NYT Politics
  - 2 NYT World

Dedup across sources is handled at the DynamoDB layer: the Stories table uses
(YearMonthDay, Title) as its composite key and save_story does a conditional
write, so the same headline appearing in multiple NYT feeds only lands once.
"""

from lib.newsdata_client import NewsdataClient
from lib.rss_client import RssClient
from lib.stories_repository import StoriesRepository


ADVICE_QUERY = '"Dear Abby" OR "Miss Manners" OR "Asking Eric" OR "Dear Annie" OR "Ask Amy"'

# Cap on newsdata API calls per source. Unused for RSS (feeds are one-shot).
MAX_API_CALLS_PER_SOURCE = 3

# Source types:
#   'newsdata_query'    -> newsdata.io /news with q=<query>
#   'newsdata_category' -> newsdata.io /news with category=<category> (None = wildcard)
#   'nyt'               -> rss.nytimes.com/services/xml/rss/nyt/<feed>.xml
#   'espn'              -> www.espn.com/espn/rss/<feed>
FETCH_PLAN = [
    {'label': 'advice',                 'n': 1, 'type': 'newsdata_query',    'query': ADVICE_QUERY},
    {'label': 'newsdata_entertainment', 'n': 2, 'type': 'newsdata_category', 'category': 'entertainment'},
    {'label': 'newsdata_wildcard',      'n': 2, 'type': 'newsdata_category', 'category': None},
    {'label': 'espn_top',               'n': 3, 'type': 'espn',              'feed': 'news'},
    {'label': 'nyt_most_viewed',        'n': 3, 'type': 'nyt',               'feed': 'MostViewed'},
    {'label': 'nyt_homepage',           'n': 3, 'type': 'nyt',               'feed': 'HomePage'},
    {'label': 'nyt_technology',         'n': 2, 'type': 'nyt',               'feed': 'Technology'},
    {'label': 'nyt_business',           'n': 2, 'type': 'nyt',               'feed': 'Business'},
    {'label': 'nyt_politics',           'n': 2, 'type': 'nyt',               'feed': 'Politics'},
    {'label': 'nyt_world',              'n': 2, 'type': 'nyt',               'feed': 'World'},
]


_newsdata = NewsdataClient()
_rss = RssClient()
_repo = StoriesRepository()


def fetch(event, context):
    """Lambda handler: fetch stories from all configured sources."""
    results = {}
    total_saved = 0

    for plan in FETCH_PLAN:
        label = plan['label']
        try:
            saved = _fetch_one(plan)
        except Exception as e:
            print(f"Error fetching {label}: {type(e).__name__}: {e}")
            saved = 0
        results[label] = saved
        total_saved += saved

    msg = f"Saved {total_saved} stories across {len(results)} sources: {results}"
    print(msg)
    return msg


def _fetch_one(plan):
    label = plan['label']
    n = plan['n']
    t = plan['type']

    print(f"--- Fetching [{label}] (type={t}, target={n}) ---")

    if t == 'newsdata_query':
        query = plan['query']
        return _fetch_newsdata_paginated(
            label, n,
            lambda page_token: _newsdata.fetch_by_query(query, use_priority=False, page_token=page_token),
        )

    if t == 'newsdata_category':
        category = plan['category']  # None for wildcard
        use_priority = category is not None
        return _fetch_newsdata_paginated(
            label, n,
            lambda page_token: _newsdata.fetch_by_category(category, use_priority, page_token=page_token),
        )

    if t == 'nyt':
        return _fetch_rss(label, n, _rss.fetch_nyt(plan['feed']))

    if t == 'espn':
        return _fetch_rss(label, n, _rss.fetch_espn(plan['feed']))

    raise ValueError(f"Unknown fetch type: {t}")


def _fetch_newsdata_paginated(label, n, fetch_page):
    """Call newsdata.io, paginating until we save n stories or hit the API-call cap."""
    saved = 0
    num_calls = 0
    page_token = None

    while saved < n and num_calls < MAX_API_CALLS_PER_SOURCE:
        response = fetch_page(page_token)
        num_calls += 1

        for story in response.get('results') or []:
            source = story.get('source_id', 'unknown')
            print(f"Processing [{label}] '{story['title']}' ({source})")
            if _repo.save_story(story, label):
                saved += 1
                if saved >= n:
                    break

        page_token = response.get('nextPage')
        if not page_token:
            break

    print(f"[{label}] saved {saved}/{n} ({num_calls} API calls)")
    return saved


def _fetch_rss(label, n, stories):
    """Try to save stories from an RSS feed in order until n are saved."""
    saved = 0
    for story in stories:
        source = story.get('source_id', 'unknown')
        print(f"Processing [{label}] '{story['title']}' ({source})")
        if _repo.save_story(story, label):
            saved += 1
            if saved >= n:
                break

    print(f"[{label}] saved {saved}/{n} from {len(stories)} feed items")
    return saved
