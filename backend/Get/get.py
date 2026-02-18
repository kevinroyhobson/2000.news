from datetime import datetime
from datetime import timedelta
from zoneinfo import ZoneInfo
import json
from decimal import Decimal
import random
import boto3
from boto3.dynamodb.conditions import Key
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_dynamo_resource = boto3.resource('dynamodb')
_headlines_table = _dynamo_resource.Table('SubvertedHeadlines')
_stories_table = _dynamo_resource.Table('Stories')
_words_table = _dynamo_resource.Table('Words')
_words_cache = {}


def get(event, context):
    logger.info("Begin get")
    logger.info(event)

    params = parse_path_params(event)
    is_today = 'day' not in params

    # Get the day to query
    if is_today:
        day_key = get_day_key()
    else:
        day_key = params['day']

    # Get headlines
    headlines = get_headlines_for_day(day_key)

    if is_today:
        # /today: pull from 3 days for cross-day pool
        now = datetime.now(ZoneInfo('America/New_York'))
        yesterday_key = get_day_key(now - timedelta(days=1))
        day_before_key = get_day_key(now - timedelta(days=2))
        headlines.extend(get_headlines_for_day(yesterday_key))
        headlines.extend(get_headlines_for_day(day_before_key))
        # Use CrossDayRank if the cross-day tournament has run, else fall back to Rank
        has_cross_day = any(h.get('CrossDayRank') is not None for h in headlines)
        rank_field = 'CrossDayRank' if has_cross_day else 'Rank'
    else:
        # /{day}: show that day only, use regular Rank
        if len(headlines) < 4:
            yesterday_key = get_day_key(datetime.now(ZoneInfo('America/New_York')) - timedelta(days=1))
            headlines.extend(get_headlines_for_day(yesterday_key))
        rank_field = 'Rank'

    # Select 4 headlines using expanding pool algorithm
    requested_headline_id = params.get('headline_slug', '')
    search_query = params.get('q', '')
    seen_as_top = set(params.get('seen', '').split(',')) if params.get('seen') else set()
    selected = select_headlines(headlines, requested_headline_id, search_query, rank_field, seen_as_top)

    # Get story details for selected headlines
    stories = enrich_with_story_details(selected, headlines, requested_headline_id)

    top_headlines = get_top_headlines(headlines, rank_field=rank_field)

    paper_name = f"The {get_random_word('adjective').capitalize()} {get_random_word('newspaper-name').capitalize()}"

    return {
        'statusCode': 200,
        'body': json.dumps({
            'PaperName': paper_name,
            'Stories': stories,
            'TopHeadlines': top_headlines,
        }, default=lambda x: int(x) if isinstance(x, Decimal) and x % 1 == 0 else float(x) if isinstance(x, Decimal) else str(x)),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
        }
    }


def parse_path_params(event):
    params = {}
    if 'pathParameters' in event and event['pathParameters'] is not None:
        params.update({k: v for (k, v) in event['pathParameters'].items()})
    if 'queryStringParameters' in event and event['queryStringParameters'] is not None:
        params.update({k: v for (k, v) in event['queryStringParameters'].items()})
    return params


def get_day_key(date=None):
    """Get day key in YYYYMMDD format, using New York timezone."""
    if date is None:
        date = datetime.now(ZoneInfo('America/New_York'))
    return date.strftime('%Y%m%d')


def get_headlines_for_day(day_key):
    """Query all headlines for a day from SubvertedHeadlines."""
    logger.info(f"Getting headlines for {day_key}")
    response = _headlines_table.query(
        KeyConditionExpression=Key('YearMonthDay').eq(day_key)
    )
    headlines = response.get('Items', [])
    logger.info(f"Got {len(headlines)} headlines for {day_key}")
    return headlines


def select_headlines(headlines, requested_headline_id, search_query='', rank_field='Rank', seen_as_top=None):
    """
    Select 4 headlines using expanding pool algorithm.
    Ensures unique StoryIds and favors higher-ranked headlines.

    If search_query is provided, matching headlines are floated to the top in random order.
    If seen_as_top is provided, the #1 pick is the highest-ranked unseen headline.

    Pool sizes: 16, 16, 32, 64 - pick one randomly from each pool, ensuring unique stories.
    rank_field: 'Rank' for daily view, 'CrossDayRank' for /today cross-day view.
    """
    if not headlines:
        return []

    if seen_as_top is None:
        seen_as_top = set()

    # Find max rank for sorting unranked items last
    max_rank = max((h.get(rank_field) or 0 for h in headlines), default=0)

    def get_rank(h):
        return h.get(rank_field) or (max_rank + 1)

    # Sort by rank
    sorted_headlines = sorted(headlines, key=get_rank)

    result = []
    picked_story_ids = set()

    # If a specific headline is requested, put it first
    if requested_headline_id:
        for h in sorted_headlines:
            if h['HeadlineId'] == requested_headline_id:
                result.append(h)
                picked_story_ids.add(h['StoryId'])
                break

    # If no requested headline and no search query, pick highest-ranked unseen headline for #1
    if not result and not search_query:
        for h in sorted_headlines:
            if h['HeadlineId'] not in seen_as_top and h['StoryId'] not in picked_story_ids:
                result.append(h)
                picked_story_ids.add(h['StoryId'])
                break
        # If all are seen, fall through to expanding pool algorithm

    # If search query provided, prioritize matching headlines (in random order)
    if search_query:
        query_lower = search_query.lower()
        matching = [
            h for h in sorted_headlines
            if h['StoryId'] not in picked_story_ids and (
                query_lower in h.get('Headline', '').lower() or
                query_lower in h.get('OriginalHeadline', '').lower()
            )
        ]
        # Shuffle matching headlines and pick unique stories
        random.shuffle(matching)
        for h in matching:
            if len(result) >= 4:
                break
            if h['StoryId'] not in picked_story_ids:
                result.append(h)
                picked_story_ids.add(h['StoryId'])

    # Expanding pool selection: pick randomly from each pool
    pool_sizes = [16, 16, 32, 64]
    for pool_size in pool_sizes:
        if len(result) >= 4:
            break
        pool = [
            h for h in sorted_headlines[:pool_size]
            if h['StoryId'] not in picked_story_ids
        ]
        if pool:
            pick = random.choice(pool)
            result.append(pick)
            picked_story_ids.add(pick['StoryId'])

    # Fill remaining slots from unpicked stories in rank order
    for h in sorted_headlines:
        if len(result) >= 4:
            break
        if h['StoryId'] not in picked_story_ids:
            result.append(h)
            picked_story_ids.add(h['StoryId'])

    return result[:4]


def enrich_with_story_details(selected_headlines, all_headlines, requested_headline_id=''):
    """Get story details from Stories table and merge with headline data."""
    if not selected_headlines:
        return []

    # Group selected headlines by YearMonthDay to batch queries
    by_day = {}
    for h in selected_headlines:
        day = h['YearMonthDay']
        if day not in by_day:
            by_day[day] = []
        by_day[day].append(h)

    # For each day, get stories and build lookup
    story_lookup = {}
    for day_key, day_headlines in by_day.items():
        story_ids_needed = {h['StoryId'] for h in day_headlines}
        stories = get_stories_for_day(day_key)
        for story in stories:
            if story.get('StoryId') in story_ids_needed:
                story_lookup[(day_key, story['StoryId'])] = story

    # Build response objects
    result = []
    for h in selected_headlines:
        story = story_lookup.get((h['YearMonthDay'], h['StoryId']), {})

        # Get sibling headlines
        siblings = [s for s in all_headlines if s['YearMonthDay'] == h['YearMonthDay'] and s['StoryId'] == h['StoryId']]
        siblings = to_headline_list(siblings)

        # Don't show original if this headline was specifically requested via URL
        show_original = False if h['HeadlineId'] == requested_headline_id else random.random() < 0.25

        result.append({
            'HeadlineId': h['HeadlineId'],
            'Headline': h['Headline'],
            'OriginalHeadline': h.get('OriginalHeadline', story.get('Title', '')),
            'ShowOriginal': show_original,
            'Angle': h.get('Angle', ''),
            'AngleSetup': h.get('AngleSetup', ''),
            'Rank': h.get('Rank'),
            'YearMonthDay': h['YearMonthDay'],
            'StoryId': h['StoryId'],
            'Description': story.get('Description', ''),
            'Url': story.get('Url', ''),
            'ImageUrl': story.get('ImageUrl', ''),
            'Source': story.get('Source', ''),
            'PublishedAt': story.get('PublishedAt', ''),
            'SiblingHeadlines': siblings,
        })

    return result


def get_stories_for_day(day_key):
    """Query all stories for a day."""
    response = _stories_table.query(
        KeyConditionExpression=Key('YearMonthDay').eq(day_key)
    )
    return response.get('Items', [])


def to_headline_list(headlines, rank_field='Rank'):
    """Transform headline objects into headline list view models."""
    return [{
        'HeadlineId': h['HeadlineId'],
        'Headline': h.get('Headline', ''),
        'Angle': h.get('Angle', ''),
        'Rank': h.get(rank_field),
        'YearMonthDay': h.get('YearMonthDay', ''),
    } for h in headlines]


def get_top_headlines(headlines, limit=64, rank_field='Rank'):
    """Get the top headlines by rank."""
    max_rank = max((h.get(rank_field) or 0 for h in headlines), default=0)
    sorted_h = sorted(headlines, key=lambda h: h.get(rank_field) or (max_rank + 1))
    return to_headline_list(sorted_h[:limit], rank_field=rank_field)


def get_random_word(word_type):
    """Get a random word of the specified type, with caching across warm invocations."""
    if word_type not in _words_cache:
        response = _words_table.query(
            KeyConditionExpression=Key('WordType').eq(word_type),
        )
        _words_cache[word_type] = [item['Word'] for item in response['Items']]
    return random.choice(_words_cache[word_type])
