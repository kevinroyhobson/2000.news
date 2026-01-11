"""
Scheduled Lambda that fetches news stories from newsdata.io.

Runs 4x/day (5am, 11am, 3pm, 8pm PT).
Fetches from 5 categories + 1 wildcard, saving up to 5 stories each.
Budget: ~6 fetches × 3 pages max = 18 API credits per run, 72/day (well within 200 free tier limit).
"""

from newsdata_client import NewsdataClient
from stories_repository import StoriesRepository


CATEGORIES = ["business", "entertainment", "sports", "technology", "politics"]
WILDCARD = "wildcard"
MAX_STORIES_PER_CATEGORY = 5
MAX_API_CALLS_PER_CATEGORY = 3

_client = NewsdataClient()
_repo = StoriesRepository()


def fetch(event, context):
    """Lambda handler: fetch stories from all categories."""
    total_saved = 0
    total_processed = 0
    results_by_category = {}

    # Fetch main categories with prioritydomain=top
    for category in CATEGORIES:
        saved, processed = fetch_category(category, use_priority=True)
        results_by_category[category] = saved
        total_saved += saved
        total_processed += processed

    # Fetch wildcard (no category, no priority filter) for variety
    saved, processed = fetch_category(WILDCARD, use_priority=False)
    results_by_category[WILDCARD] = saved
    total_saved += saved
    total_processed += processed

    result = f"Saved {total_saved} stories across {len(results_by_category)} categories: {results_by_category}"
    print(result)
    return result


def fetch_category(category, use_priority=True):
    """Fetch up to MAX_STORIES_PER_CATEGORY stories for a category."""
    saved = 0
    processed = 0
    num_api_calls = 0

    print(f"Fetching category: {category} (priority={use_priority})")

    api_category = None if category == WILDCARD else category
    response = _client.fetch_by_category(api_category, use_priority)
    num_api_calls += 1

    while True:
        if 'results' not in response or response['results'] is None:
            print(f"No results for category {category}")
            break

        for story in response['results']:
            source = story.get('source_id', 'unknown')
            print(f"Processing [{category}] '{story['title']}' ({source})")
            processed += 1

            if _repo.save_story(story, category):
                saved += 1
                if saved >= MAX_STORIES_PER_CATEGORY:
                    break

        if saved >= MAX_STORIES_PER_CATEGORY:
            break

        if 'nextPage' not in response or response['nextPage'] is None:
            break

        if num_api_calls >= MAX_API_CALLS_PER_CATEGORY:
            break

        response = _client.fetch_by_category(api_category, use_priority, response['nextPage'])
        num_api_calls += 1

    print(f"Category {category}: saved {saved}/{processed}")
    return saved, processed
