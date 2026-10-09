"""Shared repository for saving stories to DynamoDB."""

import datetime
import string
import random
import boto3
from botocore.exceptions import ClientError

# Stories sent in from the Telegram channel carry this FetchCategory prefix.
# TelegramScoop subverts them synchronously, so the Stories stream trigger
# leaves them alone.
ON_DEMAND_FETCH_PREFIX = 'telegram:'

MASHUP_FETCH_CATEGORY = 'mashup'


class StoriesRepository:
    def __init__(self, table_name='Stories'):
        self._dynamo = boto3.resource('dynamodb')
        self._table = self._dynamo.Table(table_name)

    def save_story(self, story, fetch_category, year_month_day=None, extra_attributes=None):
        """
        Save a story to DynamoDB if it has required fields and doesn't already exist. Tightly
        coupled to newsdata.io for now for simplicity.

        Args:
            story: Dict with newsdata.io story fields (title, pubDate, image_url, etc.)
            fetch_category: String identifying how this story was fetched (e.g., 'entertainment', 'manual:barack obama')
            year_month_day: Day partition to file the story under; defaults to its publish date
            extra_attributes: Additional attributes to write on the item (e.g., EditorNote)

        Returns:
            The saved item, or None if skipped (no image or already exists)
        """
        if 'image_url' not in story or story['image_url'] is None:
            print(f"Skipped story '{story['title']}' because it has no image.")
            return None

        item = {
            'YearMonthDay': year_month_day or _publish_day(story),
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
            'StoryId': _new_story_id(),
            **(extra_attributes or {}),
        }
        return self._put_if_new(item)

    def save_mashup(self, stories, editor_note):
        """Save stories the editor combined as one story, unless the same
        mashup already exists. Returns the saved item, or None if skipped."""
        return self._put_if_new(mashup_item(stories, editor_note))

    def _put_if_new(self, item):
        try:
            self._table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(YearMonthDay) AND attribute_not_exists(Title)"
            )
            return item

        except ClientError as ex:
            if ex.response['Error']['Code'] == 'ConditionalCheckFailedException':
                print(f"Skipped story '{item['Title']}' because it already exists.")
            else:
                raise ex

        return None

    def is_new(self, story):
        """Whether save_story would write this story: it has an image and
        isn't already filed under its publish day."""
        return (story.get('image_url') is not None
                and self.get_story(_publish_day(story), story['title']) is None)

    def get_story(self, year_month_day, title):
        response = self._table.get_item(Key={'YearMonthDay': year_month_day, 'Title': title})
        return response.get('Item')


def mashup_item(stories, editor_note):
    """One Stories item standing in for several. The first story's photo and
    link represent the mashup, and SourceStories keeps each real headline and
    lede for the brainstorm prompt and the story page."""
    lead = stories[0]
    sources = [{
        'Title': story['title'],
        'Description': story.get('description') or '',
        'Url': story['link'],
        'Source': story.get('source_id'),
    } for story in stories]
    return {
        'YearMonthDay': max(_publish_day(story) for story in stories),
        'PublishedAt': lead['pubDate'],
        'Title': ' / '.join(source['Title'] for source in sources),
        'Description': '\n\n'.join(source['Description'] for source in sources),
        'Url': lead['link'],
        'ImageUrl': lead['image_url'],
        'Keywords': _merged(stories, 'keywords'),
        'Category': _merged(stories, 'category'),
        'FetchCategory': MASHUP_FETCH_CATEGORY,
        'Source': ' + '.join(dict.fromkeys(source['Source'] for source in sources if source['Source'])),
        'SourceStories': sources,
        'EditorNote': editor_note,
        'RetrievedTime': datetime.datetime.now().isoformat(),
        'StoryId': _new_story_id(),
    }


def _merged(stories, field):
    """Every story's tags for field, in order; None when none have any."""
    return [tag for story in stories for tag in story.get(field) or []] or None


def _publish_day(story):
    return datetime.datetime.fromisoformat(story['pubDate']).strftime('%Y%m%d')


def _new_story_id():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
