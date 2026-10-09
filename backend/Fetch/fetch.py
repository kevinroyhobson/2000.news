"""
Scheduled Lambda that fetches news stories from a mix of sources.

Runs 4x/day. Each run saves 23 stories:
  - 2 pinned, saved straight from their source every run:
      1 advice (newsdata.io targeted query for syndicated advice columns)
      1 bengals.com Geoff Hobson story
  - 21 chosen by the assignment editor (Fetch/editor.py) from a pool of
    ~70 candidates across newsdata entertainment + wildcard, ESPN top
    stories, and NYT MostViewed / HomePage / Technology / Business /
    Politics / World.

If the editor call fails, the run falls back to fixed per-source quotas
(each plan's n), taking each source's candidates in feed order. The same
quotas backfill any slot the editor leaves empty.

Dedup across sources is handled at the DynamoDB layer: the Stories table uses
(YearMonthDay, Title) as its composite key and save_story does a conditional
write, so the same headline appearing in multiple NYT feeds only lands once.
Candidates already in the table are dropped before the editor sees them.
"""

from Fetch.editor import Candidate, pick_stories
from lib.newsdata_client import NewsdataClient
from lib.rss_client import RssClient
from lib.stories_repository import StoriesRepository


ADVICE_QUERY = '"Dear Abby" OR "Miss Manners" OR "Asking Eric" OR "Dear Annie" OR "Ask Amy"'

EDITOR_PICK_COUNT = 21

# Cap on newsdata API calls per source. Unused for RSS (feeds are one-shot).
MAX_API_CALLS_PER_SOURCE = 3

# Source types:
#   'newsdata_query'    -> newsdata.io /news with q=<query>
#   'newsdata_category' -> newsdata.io /news with category=<category> (None = wildcard)
#   'nyt'               -> rss.nytimes.com/services/xml/rss/nyt/<feed>.xml
#   'espn'              -> www.espn.com/espn/rss/<feed>
#   'bengals_hobson'    -> www.bengals.com/rss/news, Geoff Hobson stories only
#
# Pinned sources save n stories directly. Every other source offers up to
# pool candidates to the editor, and n is its quota if the editor fails.
PINNED_PLAN = [
    {'label': 'advice',         'n': 1, 'type': 'newsdata_query', 'query': ADVICE_QUERY},
    {'label': 'bengals_hobson', 'n': 1, 'type': 'bengals_hobson'},
]
EDITOR_PLAN = [
    {'label': 'newsdata_entertainment', 'n': 2, 'pool': 8,  'type': 'newsdata_category', 'category': 'entertainment'},
    {'label': 'newsdata_wildcard',      'n': 2, 'pool': 10, 'type': 'newsdata_category', 'category': None},
    {'label': 'espn_top',               'n': 3, 'pool': 8,  'type': 'espn',              'feed': 'news'},
    {'label': 'nyt_most_viewed',        'n': 3, 'pool': 10, 'type': 'nyt',               'feed': 'MostViewed'},
    {'label': 'nyt_homepage',           'n': 3, 'pool': 12, 'type': 'nyt',               'feed': 'HomePage'},
    {'label': 'nyt_technology',         'n': 2, 'pool': 6,  'type': 'nyt',               'feed': 'Technology'},
    {'label': 'nyt_business',           'n': 2, 'pool': 6,  'type': 'nyt',               'feed': 'Business'},
    {'label': 'nyt_politics',           'n': 2, 'pool': 6,  'type': 'nyt',               'feed': 'Politics'},
    {'label': 'nyt_world',              'n': 2, 'pool': 6,  'type': 'nyt',               'feed': 'World'},
]


_newsdata = NewsdataClient()
_rss = RssClient()
_repo = StoriesRepository()


def fetch(event, context):
    """Lambda handler: save the pinned stories, then the editor's picks."""
    results = {plan['label']: _save_pinned(plan) for plan in PINNED_PLAN}

    saved_picks = 0
    for candidate in save_order(gather_candidates(EDITOR_PLAN)):
        if saved_picks >= EDITOR_PICK_COUNT:
            break
        extra_attributes = {'EditorNote': candidate.note} if candidate.note else None
        if _repo.save_story(candidate.story, candidate.label, extra_attributes=extra_attributes):
            saved_picks += 1
            results[candidate.label] = results.get(candidate.label, 0) + 1

    msg = f"Saved {sum(results.values())} stories: {results}"
    print(msg)
    return msg


def save_order(candidates):
    """Every candidate, in the order to try saving them: the editor's picks,
    then each source's first n, then the rest. Fetch stops once the paper is
    full, so the backfill only fills slots the editor left empty or a save
    turned down, and covers the whole run when the editor fails."""
    picks = _editor_picks(candidates)
    picked_titles = {c.story['title'] for c in picks}
    unpicked = [c for c in candidates if c.story['title'] not in picked_titles]
    quota = quota_picks(unpicked, EDITOR_PLAN)
    quota_titles = {c.story['title'] for c in quota}
    return picks + quota + [c for c in unpicked if c.story['title'] not in quota_titles]


def _editor_picks(candidates):
    if not candidates:
        return []
    try:
        picks = pick_stories(candidates, EDITOR_PICK_COUNT)
    except Exception as e:
        print(f"Editor failed ({type(e).__name__}: {e}); falling back to per-source quotas.")
        return []

    print(f"Editor picked {len(picks)} of {len(candidates)} candidates:")
    for candidate in picks:
        print(f"  [{candidate.label}] {candidate.story['title']} — {candidate.note}")
    return picks


def quota_picks(candidates, plans):
    quotas = {plan['label']: plan['n'] for plan in plans}
    picks = []
    for candidate in candidates:
        if quotas.get(candidate.label, 0) > 0:
            quotas[candidate.label] -= 1
            picks.append(candidate)
    return picks


def gather_candidates(plans):
    """Up to each plan's pool of new, saveable stories, in source order.
    A title offered by more than one source is kept once."""
    candidates = []
    seen_titles = set()
    for plan in plans:
        label = plan['label']
        offered = 0
        try:
            for story in _stories_from(plan):
                if offered >= plan['pool']:
                    break
                if story['title'] in seen_titles or not _repo.is_new(story):
                    continue
                seen_titles.add(story['title'])
                candidates.append(Candidate(story=story, label=label))
                offered += 1
        except Exception as e:
            print(f"Error gathering {label}: {type(e).__name__}: {e}")
        print(f"[{label}] offered {offered}/{plan['pool']} candidates")
    return candidates


def _save_pinned(plan):
    """Save stories from the source in order until n are saved."""
    label = plan['label']
    saved = 0
    try:
        for story in _stories_from(plan):
            print(f"Processing [{label}] '{story['title']}' ({story.get('source_id', 'unknown')})")
            if _repo.save_story(story, label):
                saved += 1
                if saved >= plan['n']:
                    break
    except Exception as e:
        print(f"Error fetching {label}: {type(e).__name__}: {e}")
    print(f"[{label}] saved {saved}/{plan['n']}")
    return saved


def _stories_from(plan):
    t = plan['type']

    if t == 'newsdata_query':
        query = plan['query']
        return _newsdata_stories(
            lambda page_token: _newsdata.fetch_by_query(query, use_priority=False, page_token=page_token),
        )

    if t == 'newsdata_category':
        category = plan['category']  # None for wildcard
        use_priority = category is not None
        return _newsdata_stories(
            lambda page_token: _newsdata.fetch_by_category(category, use_priority, page_token=page_token),
        )

    if t == 'nyt':
        return _rss.fetch_nyt(plan['feed'])

    if t == 'espn':
        return _rss.fetch_espn(plan['feed'])

    if t == 'bengals_hobson':
        return _rss.fetch_bengals_hobson()

    raise ValueError(f"Unknown fetch type: {t}")


def _newsdata_stories(fetch_page):
    """Yield newsdata.io results page by page, up to the API-call cap. Lazy,
    so a caller that stops early never pays for the next page."""
    page_token = None
    for _ in range(MAX_API_CALLS_PER_SOURCE):
        response = fetch_page(page_token)
        yield from response.get('results') or []
        page_token = response.get('nextPage')
        if not page_token:
            return
