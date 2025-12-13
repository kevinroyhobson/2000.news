import os
import datetime
import string
import random
import boto3
from botocore.exceptions import ClientError
import requests


_news_data_api_key = os.environ['NEWS_DATA_API_KEY']
_news_data_endpoint = "https://newsdata.io/api/1/news"

# 5 main categories (prioritydomain=top for quality) + 1 wildcard (no filters for variety)
_categories = ["business", "entertainment", "sports", "technology", "politics"]
_wildcard = "wildcard"

_dynamo_resource = boto3.resource('dynamodb')
_stories_table = _dynamo_resource.Table('Stories')


def fetch(event, context):
    """
    Fetch stories from 5 categories + 1 wildcard, saving up to 5 stories each.
    Runs 4x/day (5am, 11am, 3pm, 8pm PT).
    Main categories use prioritydomain=top for top 10% of news sources.
    Wildcard has no category/priority filters to surface diverse and unusual stories.
    Budget: ~6 fetches × 3 pages max = 18 API credits per run, 72/day (well within 200 free tier limit).
    """
    total_saved = 0
    total_processed = 0
    results_by_category = {}

    # Fetch main categories with prioritydomain=top
    for category in _categories:
        saved, processed = fetch_category(category, use_priority=True)
        results_by_category[category] = saved
        total_saved += saved
        total_processed += processed

    # Fetch wildcard (no category, no priority filter) for variety
    saved, processed = fetch_category(_wildcard, use_priority=False)
    results_by_category[_wildcard] = saved
    total_saved += saved
    total_processed += processed

    result = f"Saved {total_saved} stories across {len(results_by_category)} categories: {results_by_category}"
    print(result)
    return result


def fetch_category(category, use_priority=True):
    """Fetch up to 5 stories for a category."""
    saved_for_category = 0
    processed_for_category = 0
    num_api_calls = 0

    print(f"Fetching category: {category} (priority={use_priority})")
    stories_response = fetch_stories_by_category(category if category != _wildcard else None, use_priority)
    num_api_calls += 1

    while True:
        if 'results' not in stories_response or stories_response['results'] is None:
            print(f"No results for category {category}")
            break

        for story in stories_response['results']:
            source = story.get('source_id', 'unknown')
            print(f"Processing [{category}] '{story['title']}' ({source})")
            processed_for_category += 1
            if save_story(story, category):
                saved_for_category += 1
                if saved_for_category >= 5:
                    break

        if saved_for_category >= 5:
            break

        if 'nextPage' not in stories_response or stories_response['nextPage'] is None:
            break

        if num_api_calls >= 3:  # Max 3 pages per category
            break

        stories_response = fetch_stories_by_category(
            category if category != _wildcard else None,
            use_priority,
            stories_response['nextPage']
        )
        num_api_calls += 1

    print(f"Category {category}: saved {saved_for_category}/{processed_for_category}")
    return saved_for_category, processed_for_category


def fetch_stories_by_category(category, use_priority=True, page_token=None):
    """Fetch stories from newsdata.io API."""
    query_params = f"country=us&language=en&apikey={_news_data_api_key}"

    if category is not None:
        query_params += f"&category={category}"

    if use_priority:
        query_params += "&prioritydomain=top"

    if page_token is not None:
        query_params += f"&page={page_token}"

    print(f"Fetching: {_news_data_endpoint}?{query_params.replace(_news_data_api_key, 'xxx')}")
    response = requests.get(f"{_news_data_endpoint}?{query_params}").json()

    if response['status'] == 'error':
        raise Exception(f"{response['results']['code']}: {response['results']['message']}")

    if response['status'] != 'success':
        raise Exception(f"Unexpected response status: {response['status']}")

    return response


def save_story(story, fetch_category):
    """Save a story to DynamoDB if it has required fields and doesn't already exist."""
    if 'image_url' not in story or story['image_url'] is None:
        print(f"Skipped story '{story['title']}' because it has no image.")
        return False

    try:
        _stories_table.put_item(
            Item={
                'YearMonthDay': datetime.datetime.fromisoformat(story['pubDate']).strftime('%Y%m%d'),
                'PublishedAt': story['pubDate'],
                'Title': story['title'],
                'Description': story['description'],
                'Author': story.get('creator'),
                'Content': story.get('content'),
                'Url': story['link'],
                'ImageUrl': story['image_url'],
                'VideoUrl': story.get('video_url'),
                'Language': story.get('language'),
                'Country': story.get('country'),
                'Keywords': story.get('keywords'),
                'Category': story.get('category', [fetch_category]),
                'FetchCategory': fetch_category,
                'Source': story.get('source_id'),
                'RetrievedTime': datetime.datetime.now().isoformat(),
                'StoryId': ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
            },
            ConditionExpression="attribute_not_exists(YearMonthDay) AND attribute_not_exists(Title)"
        )
        return True

    except ClientError as ex:
        if ex.response['Error']['Code'] == 'ConditionalCheckFailedException':
            print(f"Skipped story '{story['title']}' because it already exists.")
        else:
            raise ex

    return False
