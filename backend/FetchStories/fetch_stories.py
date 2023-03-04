import os
import datetime
import boto3
import requests


_news_data_api_key = os.environ['NEWS_DATA_API_KEY']
_news_data_endpoint = "https://newsdata.io/api/1/news"
_categories = ["politics", "sports", "technology", "top", "world"]

_dynamo_resource = boto3.resource('dynamodb')
_stories_table = _dynamo_resource.Table('Stories')


def fetch_stories(event, context):
    stories_response = fetch()
    for story in stories_response['results']:
        save_story(story)
        print(f"Processed story {story['title']}")

    human_readable_result = f"Processed {len(stories_response['results'])} stories of {stories_response['totalResults']} total stories."
    print(human_readable_result)
    return human_readable_result


def fetch():
    query_params = f"country=us&language=en&category={','.join(_categories)}&apikey={_news_data_api_key}"
    response = requests.get(f"{_news_data_endpoint}?{query_params}").json()

    if response['status'] == 'error':
        raise Exception(f"{response['results']['code']}: {response['results']['message']}")

    if response['status'] != 'success':
        raise Exception(f"Unexpected response status: {response['status']}")

    return response


def save_story(story):
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
        }
    )
