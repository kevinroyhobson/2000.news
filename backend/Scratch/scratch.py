import os
import boto3
import openai
from datetime import datetime, timedelta
from collections import defaultdict
import random
from dynamodb_json import json_util as dynamodb_json
from boto3.dynamodb.conditions import Key, Attr
import csv

openai.api_key = 'xxx'

_dynamo_resource = boto3.resource('dynamodb')
_words_by_word_type = None

def load_words():

    global _words_by_word_type
    _words_by_word_type = defaultdict(set)

    words_table = _dynamo_resource.Table('Words')
    response = words_table.scan()
    words = response['Items']

    for word in words:
        _words_by_word_type[word['WordType']].add(word['Word'])

def compute_subverted_titles(title, model, include_person_references):
    subverted_titles = []

    prompts = ["Rewrite this headline so that it rhymes",
               "Rewrite this headline so that it's a pun",
               "Rewrite this headline with either assonance or alliteration",
               "Rewrite this headline as a haiku and don't include a period at the end",
               "Rewrite this headline so that it's angry"]

    for prompt in prompts:
        prompt = complete_prompt(prompt, include_person_references)
        tweaked_title = replace_one_word(title)
        subverted_title = fetch_chat_gpt_rewrite(tweaked_title, prompt, model)
        subverted_titles.append(subverted_title)

    return subverted_titles


def replace_one_word(title):
    candidates = get_candidate_words_to_alter(title)
    random.Random().shuffle(candidates)

    for candidate in candidates:
        replacement_word = get_replacement_word(candidate)
        if replacement_word is not None:
            title = title.replace(candidate, replacement_word)
            print(f"Replaced {candidate} with {replacement_word}. New title: {title}")
            break

    return title


def get_candidate_words_to_alter(title):

    candidates = title.split()

    # Also check if each combination of two consecutive words together are considered a word. This is useful
    # for full names.
    for i in range(len(candidates) - 1):
        candidates.append(f"{candidates[i]} {candidates[i + 1]}")

    candidates = [c for c in candidates if len(c) > 3]
    return candidates


def get_replacement_word(word):

    for word_type in _words_by_word_type.keys():
        if word in _words_by_word_type[word_type]:
            return random.choice(list(_words_by_word_type[word_type]))

    return None


def complete_prompt(prompt, use_person_references):

    word_types_to_replace = ['noun']
    if (use_person_references):
        word_types_to_replace.append('person')

    random_reference_word_type = random.choice(word_types_to_replace)
    random_reference = random.choice(list(_words_by_word_type[random_reference_word_type]))

    reference_phrases = ["and include a reference to",
                         "and include an homage to",
                         "and include a"]

    return f"{prompt}, {random.choice(reference_phrases)} {random_reference}:"


def fetch_chat_gpt_rewrite(title, prompt, model):
    response = openai.ChatCompletion.create(
        model=model,
        messages=[{"role": "system", "content": "You are a copywriter who writes short headlines in a pithy, succinct, funny, satirical style like the New York Post."},
                  {"role": "user", "content": f"{prompt} {title}"}],
        temperature=1.1,
        max_tokens=50,
        frequency_penalty=0.5,
        presence_penalty=-0.5,
    )
    subverted_title = response['choices'][0]['message']['content']
    subverted_title = subverted_title.strip()
    if subverted_title.startswith("\"") and subverted_title.endswith("\""):
        subverted_title = subverted_title[1:-1]

    print(f"{model} subverted title: {subverted_title} (used {response['usage']['total_tokens']} tokens)")

    return {
        'SubvertedTitle': subverted_title,
        'Prompt': f"{prompt} {title}",
        'TotalTokens': response['usage']['total_tokens']
    }

def get_recent_stories():

    recent_stories = get_stories_for_date(datetime.today())
    if len(recent_stories) < 5:
        yesterday = datetime.today() - datetime.timedelta(days=1)
        yesterday_stories = get_stories_for_date(yesterday)
        recent_stories.extend(yesterday_stories)

    return random.sample(recent_stories, 5)
    # return recent_stories

def get_stories_for_date(date):
    response = _stories_table.query(
        KeyConditionExpression=Key('YearMonthDay').eq(date.strftime('%Y%m%d')),
        FilterExpression=Attr('SubvertedTitles').exists()
    )
    stories = response['Items']

    return stories

def compare_models(stories):

    print("Real title: " + stories[0]['Title'])

    with open('scratch.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Original headline", "Source", "Model", "Use person refs?"])
        
        for story in stories:

            models = ["gpt-3.5-turbo", "gpt-4", "gpt-4-turbo-preview"]
            use_person_refs = [True, False]

            for model in models:
                for use_person_ref in use_person_refs:
                    subverted_titles = compute_subverted_titles(story['Title'], model, use_person_ref)

                    row = [story['Title'], story['Source'], model, use_person_ref]
                    for subverted_title in subverted_titles:
                        row.append(subverted_title['SubvertedTitle'])

                    writer.writerow(row)

def analyze_sources(stories):
    with open(f'analyses/{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Original headline", "Source", "Category", "Keywords"])

        for story in stories:
            writer.writerow([story['Title'], story['Source'], story['Category'], story['Keywords']])

load_words()

_stories_table = _dynamo_resource.Table('Stories')
# stories = []
for i in range(0, 2):
    # stories.extend(get_stories_for_date(datetime.today() - timedelta(days=i)))
    # analyze_sources(stories)
    stories = get_recent_stories()
    compare_models(stories)
