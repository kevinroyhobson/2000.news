"""
Tournament Lambda - Cross-story headline ranking

Triggered by DynamoDB stream on SubvertedHeadlines (batched ~5 min after new headlines).
Ranks all today's headlines via batch ranking tournament.

Uses batch ranking (groups of ~10, rank all, top 3 advance) with all-Opus for quality.
Cuts API calls from ~420 to ~60 for 420 headlines while giving the model real competitive
context for calibrated judging.

Ranking system:
- Final group (≤20 remain): explicit 1-through-N ordering
- Earlier rounds: sub-ranked within tiers by intra-group finish position
  (4th-place finishers rank above 5th, above 6th, etc.)
"""

import os
import time
import datetime
import random
from itertools import groupby
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from boto3.dynamodb.conditions import Key
import anthropic
from langfuse import get_client, observe
from lib.ssm_secrets import get_secret


_dynamo_resource = boto3.resource('dynamodb')
_headlines_table = _dynamo_resource.Table('SubvertedHeadlines')
os.environ.setdefault("LANGFUSE_PUBLIC_KEY", get_secret("LANGFUSE_PUBLIC_KEY"))
os.environ.setdefault("LANGFUSE_SECRET_KEY", get_secret("LANGFUSE_SECRET_KEY"))
os.environ.setdefault("LANGFUSE_HOST", "https://us.cloud.langfuse.com")
langfuse = get_client()

_anthropic_client = None


def get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = anthropic.Anthropic(api_key=get_secret("ANTHROPIC_API_KEY"))
    return _anthropic_client


@observe()
def tournament(event, context):
    """
    Main handler - triggered by DynamoDB stream on SubvertedHeadlines.
    Runs cross-story tournament to rank all today's headlines.
    """
    try:
        today = datetime.datetime.now(ZoneInfo('America/New_York')).strftime('%Y%m%d')
        print(f"Running tournament for {today}")

        # 1. Query all headlines for today from SubvertedHeadlines
        all_headlines = get_headlines_for_day(today)
        print(f"Found {len(all_headlines)} headlines for {today}")

        if len(all_headlines) < 2:
            print("Not enough headlines for tournament")
            if len(all_headlines) == 1:
                update_headline_rank(today, all_headlines[0]['headline_id'], 1)
            return f"Only {len(all_headlines)} headlines for {today}"

        # 2. Run batch ranking tournament - everyone gets a rank
        headlines_by_rank = run_tournament(all_headlines)
        print(f"Tournament complete, ranks assigned: {headlines_by_rank}")

        # 3. Update Rank field on each headline in SubvertedHeadlines
        update_all_ranks(today, headlines_by_rank)

        rank_summary = {k: len(v) for k, v in headlines_by_rank.items()}
        return f"Tournament complete for {today}: {len(all_headlines)} headlines ranked. Distribution: {rank_summary}"
    finally:
        langfuse.flush()


def get_headlines_for_day(day_key: str) -> list:
    """Query all headlines for a given day from SubvertedHeadlines."""
    response = _headlines_table.query(
        KeyConditionExpression=Key('YearMonthDay').eq(day_key)
    )
    items = response.get('Items', [])

    # Convert to format expected by tournament
    return [{
        'headline': item['Headline'],
        'headline_id': item['HeadlineId'],
        'angle': item.get('Angle', ''),
        'angle_setup': item.get('AngleSetup', ''),
        'original_headline': item.get('OriginalHeadline', ''),
        'story_id': item.get('StoryId', ''),
        'year_month_day': item['YearMonthDay'],
    } for item in items]


def run_tournament(candidates: list) -> dict:
    """
    Run batch ranking tournament. Every headline gets a rank.

    Elimination rounds (while remaining > 20):
      Divide into groups of ~10, rank each group, top 3 advance.
    Final round (≤20 remain):
      Rank all in one call → explicit 1-through-N ordering.

    Returns dict: {rank_str: [headline_ids]}
    """
    random.shuffle(candidates)
    remaining = candidates.copy()

    # Each entry: list of (headline_id, intra_group_position)
    # Stored in order so we can reverse (most recent round first) for rank assignment
    elimination_rounds = []

    round_num = 1
    while len(remaining) > 20:
        num_groups = round(len(remaining) / 10)
        groups = distribute_into_groups(remaining, num_groups)

        print(f"--- Round {round_num} ({len(remaining)} headlines, {num_groups} groups of ~{len(remaining) // num_groups}) ---")

        next_remaining = []
        round_eliminated = []

        with ThreadPoolExecutor(max_workers=min(50, num_groups)) as executor:
            futures = {
                executor.submit(rank_group, group, round_num, len(remaining)): i
                for i, group in enumerate(groups)
            }

            for future in as_completed(futures):
                ordered, explanation = future.result()
                winners = ordered[:3]
                losers = ordered[3:]

                next_remaining.extend(winners)
                for pos, headline in enumerate(losers, start=3):
                    round_eliminated.append((headline['headline_id'], pos))

                winner_preview = [h['headline'][:40] for h in winners]
                print(f"  Group ({len(ordered)}): winners={winner_preview}")

        elimination_rounds.append(round_eliminated)
        remaining = next_remaining
        round_num += 1

    # Final round — rank all remaining in one call
    print(f"--- Final round ({len(remaining)} headlines) ---")
    final_ordered, explanation = rank_group(remaining, round_num, len(remaining))

    # Assign ranks: final group gets explicit 1-N ordering
    headlines_by_rank = {}
    current_rank = 1

    for headline in final_ordered:
        headlines_by_rank[str(current_rank)] = [headline['headline_id']]
        current_rank += 1

    # Earlier rounds in reverse order (most recent first = closest to final)
    for round_eliminated in reversed(elimination_rounds):
        round_eliminated.sort(key=lambda x: x[1])
        for _pos, pos_group in groupby(round_eliminated, key=lambda x: x[1]):
            ids = [hid for hid, _ in pos_group]
            headlines_by_rank[str(current_rank)] = ids
            current_rank += 1

    print(f"Final rankings: {headlines_by_rank}")
    return headlines_by_rank


def distribute_into_groups(items: list, num_groups: int) -> list:
    """Distribute items as evenly as possible into num_groups groups (sizes 8-12)."""
    base_size = len(items) // num_groups
    remainder = len(items) % num_groups

    groups = []
    start = 0
    for i in range(num_groups):
        size = base_size + (1 if i < remainder else 0)
        groups.append(items[start:start + size])
        start += size

    return groups


def rank_group(group: list, round_num: int, remaining: int) -> tuple:
    """
    Ask the model to rank a group of headlines from best to worst.
    Returns (ordered_headlines, explanation).
    """
    labels = [chr(ord('A') + i) for i in range(len(group))]

    headline_lines = []
    for label, h in zip(labels, group):
        headline_lines.append(f'{label}: "{h["headline"]}"')
        headline_lines.append(f'  Original: "{h["original_headline"]}"')

    is_late = remaining <= 40
    if is_late:
        explanation_instruction = (
            "explain your reasoning — what made the top picks stand out "
            "and what held others back (3-5 sentences)"
        )
    else:
        explanation_instruction = (
            "briefly note what made your top pick stand out (1 sentence)"
        )

    headline_block = '\n'.join(headline_lines)
    prompt = f"""Rank these satirical headlines from best to worst. Consider the original news and comedic approach.

{headline_block}

Value CRAFT as much as humor:
- Clever alliteration or assonance
- Puns that actually work phonetically
- Unexpected wordplay or double meanings
- Rhythm and flow when read aloud
- How well the joke plays off the original headline

Reply with ALL letters in order from best to worst, separated by commas.
Then on a new line, {explanation_instruction}."""

    max_tokens = 400 if is_late else 200

    response_text = call_tournament_model(
        prompt, max_tokens=max_tokens,
        round_num=round_num, remaining=remaining,
    )

    # Parse: first line = comma-separated letters, rest = explanation
    lines = response_text.strip().split('\n')
    ranking_line = lines[0]
    explanation = '\n'.join(lines[1:]).strip()

    valid_max = chr(ord('A') + len(group) - 1)
    letters = [c.strip().upper() for c in ranking_line.split(',')]
    valid = [l for l in letters if len(l) == 1 and 'A' <= l <= valid_max]

    label_to_headline = {chr(ord('A') + i): h for i, h in enumerate(group)}

    ordered = []
    seen = set()
    for letter in valid:
        if letter not in seen:
            ordered.append(label_to_headline[letter])
            seen.add(letter)

    # Append unmentioned headlines in random order
    unmentioned = [h for i, h in enumerate(group) if chr(ord('A') + i) not in seen]
    if unmentioned:
        random.shuffle(unmentioned)
        ordered.extend(unmentioned)

    # If completely unparseable, shuffle randomly
    if not valid:
        print(f"Unparseable ranking response, shuffling: {ranking_line}")
        random.shuffle(ordered)

    return ordered, explanation


MAX_RETRIES = 4
RETRY_BASE_DELAY = 2  # seconds


@observe(as_type="generation")
def call_tournament_model(prompt: str, max_tokens=200,
                          round_num=0, remaining=0) -> str:
    """Call the tournament model. All calls use Opus and are traced in Langfuse."""
    provider = os.getenv("TOURNAMENT_PROVIDER", "anthropic")
    model = os.getenv("TOURNAMENT_MODEL", "claude-opus-4-6")
    text, usage = _do_api_call(provider, model, prompt, max_tokens=max_tokens)
    langfuse.update_current_generation(
        model=model,
        usage_details=usage,
        metadata={"round_num": round_num, "remaining_count": remaining},
    )
    return text


def _do_api_call(provider: str, model: str, prompt: str,
                 max_tokens: int = 200) -> tuple:
    """Make the actual API call with retries. Returns (text, usage_dict)."""
    print(f"[tournament] Calling {provider}/{model} (max_tokens={max_tokens})")

    for attempt in range(MAX_RETRIES + 1):
        try:
            if provider == "anthropic":
                client = get_anthropic_client()
                response = client.messages.create(
                    model=model,
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = response.content[0].text
                usage = {
                    "input_tokens": response.usage.input_tokens,
                    "output_tokens": response.usage.output_tokens,
                }
            else:
                raise ValueError(f"Unknown provider: {provider}")

            return text, usage

        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
            print(f"[tournament] Retry {attempt + 1}/{MAX_RETRIES} after {delay:.1f}s: {e}")
            time.sleep(delay)


def update_all_ranks(day_key: str, headlines_by_rank: dict):
    """Update Rank field on each headline in SubvertedHeadlines."""
    updated_count = 0
    for rank_str, headline_ids in headlines_by_rank.items():
        rank = int(rank_str)
        for headline_id in headline_ids:
            update_headline_rank(day_key, headline_id, rank)
            updated_count += 1
    print(f"Updated ranks for {updated_count} headlines")


def update_headline_rank(day_key: str, headline_id: str, rank: int):
    """Update the Rank field on a single headline."""
    _headlines_table.update_item(
        Key={'YearMonthDay': day_key, 'HeadlineId': headline_id},
        UpdateExpression='SET #r = :rank',
        ExpressionAttributeNames={'#r': 'Rank'},
        ExpressionAttributeValues={':rank': rank},
    )


if __name__ == "__main__":
    # Test locally
    from dotenv import load_dotenv
    load_dotenv()

    result = tournament({}, {})
    print(result)
    langfuse.flush()
