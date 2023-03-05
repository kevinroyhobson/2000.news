import os
import datetime
import json
import random
import boto3
from boto3.dynamodb.conditions import Key, Attr

_dynamo_resource = boto3.resource('dynamodb')
_stories_table = _dynamo_resource.Table('Stories')
_words_table = _dynamo_resource.Table('Words')

def get(event, context):

    recent_stories = get_recent_stories()
    recent_stories = [get_story_view_model(story) for story in recent_stories]
    paper_name = f"The {get_random_word_of_type('adjective').capitalize()} {get_random_word_of_type('newspaper-name').capitalize()}"

    return {
        'statusCode': 200,
        'body': json.dumps({
            'PaperName': paper_name,
            'Stories': recent_stories,
        }),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
        }
    }

def get_recent_stories():

    recent_stories = get_stories_for_date(datetime.datetime.today())
    if len(recent_stories) < 5:
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        yesterday_stories = get_stories_for_date(yesterday)
        recent_stories.extend(yesterday_stories)

    return random.sample(recent_stories, 5)

def get_stories_for_date(date):
    response = _stories_table.query(
        KeyConditionExpression=Key('YearMonthDay').eq(date.strftime('%Y%m%d')),
        FilterExpression=Attr('SubvertedTitles').exists()
    )
    stories = response['Items']

    return stories


def get_story_view_model(story):

    story['OriginalTitle'] = story['Title']

    subversion_rate = float(os.environ['SUBVERSION_RATE'])
    if subversion_rate > random.random():
        subverted_titles_to_use = [title['SubvertedTitle'] for title in story['SubvertedTitles'] if not is_ai_apology(title['SubvertedTitle'])]

        if any(subverted_titles_to_use):
            story['Title'] = random.choice(subverted_titles_to_use)

    del story['SubvertedTitles']
    return story


def is_ai_apology(title):
    phrases_to_exclude = ["an AI language model",
                          "I cannot perform this task",
                          "I cannot do this task",
                          "inappropriate content",
                          "offensive content",
                          "OpenAI's content policy",
                          "I cannot fulfill this request"]
    return any([phrase in title for phrase in phrases_to_exclude])


def get_random_word_of_type(word_type):
    words = _words_table.query(
        KeyConditionExpression=Key('WordType').eq(word_type),
    )['Items']
    return random.choice(words)['Word']
