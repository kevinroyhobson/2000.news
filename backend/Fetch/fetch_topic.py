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
from lib.topic_search import save_stories_for_query


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

    saved = save_stories_for_query(
        query, StoriesRepository(), NewsdataClient(), f'manual:{query}',
        max_stories=max, use_priority=not no_priority,
    )

    print()
    print(f"Done! Saved {len(saved)} stories for '{query}'")
    if saved:
        print("Stories will be processed by the Subvert pipeline automatically.")


if __name__ == '__main__':
    main()
