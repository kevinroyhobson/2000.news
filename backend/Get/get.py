from datetime import datetime
from datetime import timedelta
import json
import random
import boto3
from boto3.dynamodb.conditions import Key, Attr
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info('Begin executing python')

_dynamo_resource = boto3.resource('dynamodb')
_stories_table = _dynamo_resource.Table('Stories')
_words_table = _dynamo_resource.Table('Words')

logger.info('End static dynamo setup')

def get(event, context):

    logger = logging.getLogger()
    logger.info("Begin get")

    params = parse_path_params(event)
    logger.info(event)
    isDebugMode = 'queryStringParameters' in event and event['queryStringParameters'] is not None and event['queryStringParameters'].get('debug') == 'true'

    stories = get_stories_for_day(params['day']) if 'day' in params else get_recent_stories()
    story_view_model_lists = [get_headline_view_models(story) for story in stories]
    story_view_models = [view_model for view_models in story_view_model_lists for view_model in view_models]
    story_view_models = order_stories(story_view_models, params.get('headline_slug', ''))

    paper_name = f"The {get_random_word_of_type('adjective').capitalize()} {get_random_word_of_type('newspaper-name').capitalize()}"

    retVal = {
        'statusCode': 200,
        'body': json.dumps({
            'PaperName': paper_name,
            'Stories': story_view_models if isDebugMode else story_view_models[:4],
        }),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
        }
    }

    logger.info("End get")
    return retVal


def parse_path_params(event):
    if 'pathParameters' in event and event['pathParameters'] is not None:
        return {k:v for (k, v) in event['pathParameters'].items()}
        
    return {}


def get_recent_stories():
    
    recent_stories = get_stories_for_day(get_day_key(datetime.today()))
    if len(recent_stories) < 4:
        yesterday = datetime.today() - timedelta(days=1)
        yesterday_stories = get_stories_for_day(get_day_key(yesterday))
        recent_stories.extend(yesterday_stories)

    return recent_stories


def get_day_key(date):
    return date.strftime('%Y%m%d')

def get_stories_for_day(day_key):

    logger.info(f"Getting stories for {day_key}")

    response = _stories_table.query(
        KeyConditionExpression=Key('YearMonthDay').eq(day_key),
        FilterExpression=Attr('SubvertedTitles').exists()
    )

    logger.info(f"Got {len(response['Items'])} stories for {day_key}")
    return response['Items']


def get_headline_view_models(story):

    headlines = []
    day = story['YearMonthDay']

    # Build list of all headline options for this story (for sibling links)
    all_headline_options = []
    for title in story['SubvertedTitles']:
        if not is_ai_apology(title['SubvertedTitle']):
            all_headline_options.append({
                'Headline': title['SubvertedTitle'],
                'HeadlineId': title['SubvertedTitleId'],
                'Angle': title.get('Angle', ''),
                'AngleSetup': title.get('AngleSetup', ''),
            })
    # Add the original headline as an option too
    all_headline_options.append({
        'Headline': story['Title'],
        'HeadlineId': story['StoryId'],
        'Angle': 'original',
        'AngleSetup': '',
    })

    for title in story['SubvertedTitles']:
        if not is_ai_apology(title['SubvertedTitle']):
            headline = story.copy()
            headline['Headline'] = title['SubvertedTitle']
            headline['HeadlineId'] = title['SubvertedTitleId']
            headline['OriginalHeadline'] = story['Title']
            headline['SiblingHeadlines'] = [h for h in all_headline_options if h['HeadlineId'] != title['SubvertedTitleId']]
            del headline['SubvertedTitles']
            del headline['Title']
            headlines.append(headline)

    story['Headline'] = story['Title']
    story['HeadlineId'] = story['StoryId']
    story['OriginalHeadline'] = story['Title']
    story['SiblingHeadlines'] = [h for h in all_headline_options if h['HeadlineId'] != story['StoryId']]
    del story['SubvertedTitles']
    del story['Title']
    headlines.append(story)

    return headlines


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

    logger.info(f"Getting words of type {word_type}")
    
    words = _words_table.query(
        KeyConditionExpression=Key('WordType').eq(word_type),
    )['Items']
    
    logger.info(f"Got {len(words)} words of type {word_type}")
    return random.choice(words)['Word']


def order_stories(stories, top_headline_id):
    
    random.shuffle(stories)
    stories = bring_element_to_front(stories, 
                                     lambda story: story['HeadlineId'] == top_headline_id)
    
    return stories


def bring_element_to_front(list, predicate):
    for i, item in enumerate(list):
        if predicate(item):
            list.insert(0, list.pop(i))
            break

    return list
