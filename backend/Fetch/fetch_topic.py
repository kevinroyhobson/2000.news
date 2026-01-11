#!/usr/bin/env python3
"""
Fetch articles for a specific topic/person and add them to the Stories table.
This triggers the Subvert pipeline automatically via DynamoDB streams.

Usage:
    python fetch_topic.py "barack obama"
    python fetch_topic.py "climate summit" --max 3
    python fetch_topic.py "Taylor Swift" --no-priority
"""

import click
from lib.newsdata_client import NewsdataClient
from lib.stories_repository import StoriesRepository


MAX_API_CALLS = 3


@click.command()
@click.argument('query')
@click.option('--max', default=3, help='Maximum stories to save.')
@click.option('--no-priority', is_flag=True, help='Include lower-tier sources.')
def main(query, max, no_priority):
    """Fetch articles for QUERY and add to the news pipeline."""
    print(f"Searching for: {query}")
    print(f"Max stories: {max}")
    print(f"Priority sources only: {not no_priority}")
    print()

    client = NewsdataClient()
    repo = StoriesRepository()

    saved = 0
    processed = 0
    num_api_calls = 0

    response = client.fetch_by_query(query, use_priority=not no_priority)
    num_api_calls += 1

    while True:
        if 'results' not in response or response['results'] is None:
            print("No results found.")
            break

        for story in response['results']:
            source = story.get('source_id', 'unknown')
            print(f"[{source}] {story['title']}")
            processed += 1

            if repo.save_story(story, f'manual:{query}'):
                saved += 1

            if saved >= max:
                break

        if saved >= max:
            break

        if 'nextPage' not in response or response['nextPage'] is None:
            break

        if num_api_calls >= MAX_API_CALLS:
            print("Reached max API calls (3 pages)")
            break

        response = client.fetch_by_query(query, use_priority=not no_priority,
                                         page_token=response['nextPage'])
        num_api_calls += 1

    print()
    print(f"Done! Saved {saved}/{processed} stories for '{query}'")
    if saved > 0:
        print("Stories will be processed by the Subvert pipeline automatically.")


if __name__ == '__main__':
    main()
