import os
import datetime
import string
import random
import boto3
from botocore.exceptions import ClientError
import requests


_news_data_api_key = os.environ['NEWS_DATA_API_KEY']
_news_data_endpoint = "https://newsdata.io/api/1/news"
_sources = ["abcnews",
            "bloomberg",
            "deadline",
            "financialtimes",
            "kron4",
            "latimes",
            "nasa",
            "nbcnews",
            "news-medical",
            "newyorkcbslocal",
            "nytimes",
            "si",
            "tmz",
            "usnews",
            "venturebeat",
            "washingtonpost"]

_dynamo_resource = boto3.resource('dynamodb')
_stories_table = _dynamo_resource.Table('Stories')


def fetch(event, context):
    this_instance_sources = random.sample(_sources, 5)
    stories_response = fetch_stories(this_instance_sources)

    num_stories_saved = 0
    num_stories_processed = 0
    num_api_calls = 1

    while True:
        for story in stories_response['results']:
            print(f"Processing story '{story['title']}' ({story['source_id']})")
            num_stories_processed += 1
            if save_story(story):
                num_stories_saved += 1

        if num_stories_saved >= 5:
            break

        if 'nextPage' not in stories_response or stories_response['nextPage'] is None:
            break

        if num_api_calls >= 4:
            break

        stories_response = fetch_stories(this_instance_sources, stories_response['nextPage'])
        num_api_calls += 1

    human_readable_result = f"Processed {num_stories_processed} stories of {stories_response['totalResults']} total stories. Saved {num_stories_saved} stories."
    print(human_readable_result)
    return human_readable_result


def fetch_stories(sources, page_token=None):
    query_params = f"country=us&language=en&domain={','.join(sources)}&apikey={_news_data_api_key}"
    if page_token is not None:
        query_params += f"&page={page_token}"

    print(f"Fetching stories with query params: {query_params}")
    response = requests.get(f"{_news_data_endpoint}?{query_params}").json()

    if response['status'] == 'error':
        raise Exception(f"{response['results']['code']}: {response['results']['message']}")

    if response['status'] != 'success':
        raise Exception(f"Unexpected response status: {response['status']}")

    return response


def save_story(story):
    if 'source_id' not in story or story['source_id'] not in _sources:
        print(f"Skipped story '{story['title']}' because it has no source.")
        return False

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
                'Author': story['creator'],
                'Content': story['content'],
                'Url': story['link'],
                'ImageUrl': story['image_url'],
                'VideoUrl': story['video_url'],
                'Language': story['language'],
                'Country': story['country'],
                'Keywords': story['keywords'],
                'Category': story['category'],
                'Source': story['source_id'],
                'RetrievedTime': datetime.datetime.now().isoformat(),
                'StoryId': ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
            },
            ConditionExpression="attribute_not_exists(YearMonthDay) AND attribute_not_exists(Title)"
        )
        return True

    except ClientError as ex:
        if ex.response['Error']['Code'] == 'ConditionalCheckFailedException':
            print(f"Skipped story '{story['title']}' ({story['source_id']}) because it already exists.")
        else:
            raise ex

    return False
