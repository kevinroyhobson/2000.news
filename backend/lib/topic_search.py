"""Search newsdata.io for a topic and file the results as stories.

Shared by the fetch_topic CLI and the Telegram /scoop command.
"""

MAX_API_CALLS = 3


def save_stories_for_query(query, client, save, max_stories=3, use_priority=True):
    """Page through search results until max_stories are saved or the API-call
    cap is hit. save(story) returns the saved item, or None to skip it.
    Returns the saved items."""
    saved = []
    page_token = None

    for _ in range(MAX_API_CALLS):
        response = client.fetch_by_query(query, use_priority=use_priority, page_token=page_token)
        for story in response.get('results') or []:
            print(f"[{story.get('source_id', 'unknown')}] {story['title']}")
            item = save(story)
            if item:
                saved.append(item)
            if len(saved) >= max_stories:
                return saved

        page_token = response.get('nextPage')
        if not page_token:
            break

    return saved
