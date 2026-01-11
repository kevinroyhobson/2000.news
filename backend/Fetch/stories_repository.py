"""Shared repository for saving stories to DynamoDB."""

import datetime
import string
import random
import boto3
from botocore.exceptions import ClientError


class StoriesRepository:
    def __init__(self, table_name='Stories'):
        self._dynamo = boto3.resource('dynamodb')
        self._table = self._dynamo.Table(table_name)

    def save_story(self, story, fetch_category):
        """
        Save a story to DynamoDB if it has required fields and doesn't already exist. Tightly
        coupled to newsdata.io for now for simplicity.

        Args:
            story: Dict with newsdata.io story fields (title, pubDate, image_url, etc.)
            fetch_category: String identifying how this story was fetched (e.g., 'entertainment', 'manual:barack obama')

        Returns:
            True if saved, False if skipped (no image or already exists)
        """
        if 'image_url' not in story or story['image_url'] is None:
            print(f"Skipped story '{story['title']}' because it has no image.")
            return False

        try:
            self._table.put_item(
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
