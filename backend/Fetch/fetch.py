import os
import datetime
import boto3
from botocore.exceptions import ClientError
import requests


_news_data_api_key = os.environ['NEWS_DATA_API_KEY']
_news_data_endpoint = "https://newsdata.io/api/1/news"
_categories = ["politics", "sports", "technology", "top", "world"]

_dynamo_resource = boto3.resource('dynamodb')
_stories_table = _dynamo_resource.Table('Stories')


def fetch(event, context):
    stories_response = fetch_stories()
    num_stories_saved = 0
    for story in stories_response['results']:
        print(f"Processing story '{story['title']}'")
        if save_story(story):
            num_stories_saved += 1

    human_readable_result = f"Processed {len(stories_response['results'])} stories of {stories_response['totalResults']} total stories. Saved {num_stories_saved} stories."
    print(human_readable_result)
    return human_readable_result


def fetch_stories():
    query_params = f"country=us&language=en&category={','.join(_categories)}&apikey={_news_data_api_key}"
    response = requests.get(f"{_news_data_endpoint}?{query_params}").json()

    if response['status'] == 'error':
        raise Exception(f"{response['results']['code']}: {response['results']['message']}")

    if response['status'] != 'success':
        raise Exception(f"Unexpected response status: {response['status']}")

    return response


def save_story(story):
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
                'RetrievedTime': datetime.datetime.now().isoformat()
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
