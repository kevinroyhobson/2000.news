"""
Tournament Lambda - Cross-story headline ranking

Runs hourly to rank all headlines from today's stories via pairwise tournament.
Results are written to DailyHeadlineRankings table.

Ranking system:
- Rank 1: Tournament winner
- Rank 2: Lost in final
- Rank 3: Lost in semifinal (2 headlines)
- Rank 4: Lost in quarterfinal (4 headlines)
- And so on for all headlines
"""

import os
import datetime
import random
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from boto3.dynamodb.conditions import Key
from google import genai
from google.genai import types
import anthropic


_dynamo_resource = boto3.resource('dynamodb')
_headlines_table = _dynamo_resource.Table('SubvertedHeadlines')

_anthropic_client = None
_google_client = None


def get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    return _anthropic_client


def get_google_client():
    global _google_client
    if _google_client is None:
        _google_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    return _google_client


def tournament(event, context):
    """
    Main handler - runs hourly tournament across all today's headlines.
    """
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

    # 2. Run elimination tournament - everyone gets a rank
    headlines_by_rank = run_tournament(all_headlines)
    print(f"Tournament complete, ranks assigned: {headlines_by_rank}")

    # 3. Update Rank field on each headline in SubvertedHeadlines
    update_all_ranks(today, headlines_by_rank)

    rank_summary = {k: len(v) for k, v in headlines_by_rank.items()}
    return f"Tournament complete for {today}: {len(all_headlines)} headlines ranked. Distribution: {rank_summary}"


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
    Run full elimination tournament. Every headline gets a rank.

    Returns dict: {rank: [headline_ids]}
    - Rank 1: Winner
    - Rank 2: Lost in final
    - Rank 3: Lost in semifinal
    - etc.
    """
    # Shuffle to avoid position bias
    random.shuffle(candidates)
    current_round = candidates.copy()

    # Track eliminated headlines by round
    # eliminated[round_num] = [headline_ids eliminated in that round]
    eliminated = {}

    round_num = 1
    while len(current_round) > 1:
        print(f"--- Tournament Round {round_num} ({len(current_round)} competitors) ---")
        next_round = []
        round_losers = []

        # Build list of pairs for this round
        pairs = []
        for i in range(0, len(current_round) - 1, 2):
            pairs.append((current_round[i], current_round[i + 1]))

        # Process pairs in parallel
        with ThreadPoolExecutor(max_workers=min(20, len(pairs))) as executor:
            future_to_pair = {executor.submit(compare_pair, a, b): (a, b) for a, b in pairs}
            for future in as_completed(future_to_pair):
                a, b = future_to_pair[future]
                winner = future.result()
                loser = b if winner['headline_id'] == a['headline_id'] else a
                next_round.append(winner)
                round_losers.append(loser['headline_id'])
                print(f"  WIN: {winner['headline'][:50]}...")
                print(f"  lose: {loser['headline'][:50]}...")

        # If odd number, last one gets a bye
        if len(current_round) % 2 == 1:
            bye = current_round[-1]
            next_round.append(bye)
            print(f"  BYE: {bye['headline'][:50]}...")

        eliminated[round_num] = round_losers
        current_round = next_round
        round_num += 1

    # Winner is the last one standing
    winner_id = current_round[0]['headline_id']
    total_rounds = round_num - 1  # -1 because we incremented after the final round

    # Convert elimination rounds to ranks
    # Rank 1 = winner
    # Rank 2 = lost in final (round = total_rounds)
    # Rank 3 = lost in semifinal (round = total_rounds - 1)
    # etc.
    # Use string keys for DynamoDB compatibility
    headlines_by_rank = {"1": [winner_id]}

    for elim_round, loser_ids in eliminated.items():
        # rank = (total_rounds - elim_round) + 2
        # final = total_rounds → rank 2
        # semifinal = total_rounds-1 → rank 3
        rank = total_rounds - elim_round + 2
        headlines_by_rank[str(rank)] = loser_ids

    print(f"Final rankings: {headlines_by_rank}")
    return headlines_by_rank


def compare_pair(a: dict, b: dict) -> dict:
    """
    Ask the model which headline is funnier, with full context about
    the original story and comedic angle.
    """
    prompt = f"""Which satirical headline is better? Consider the original news and the comedic approach.

HEADLINE A: "{a['headline']}"
  Original: "{a['original_headline']}"
  Comedic angle: {a['angle']}
  Approach: {a['angle_setup']}

HEADLINE B: "{b['headline']}"
  Original: "{b['original_headline']}"
  Comedic angle: {b['angle']}
  Approach: {b['angle_setup']}

Value CRAFT as much as humor:
- Clever alliteration or assonance
- Puns that actually work phonetically
- Unexpected wordplay or double meanings
- Rhythm and flow when read aloud
- How well the joke plays off the original headline

A straightforward joke that lands is good, but a headline with clever linguistic craft is equally valuable.

Reply with ONLY the letter A or B, nothing else."""

    response_text = call_tournament_model(prompt).strip().upper()

    # Parse response - look for A or B
    if "A" in response_text and "B" not in response_text:
        return a
    elif "B" in response_text and "A" not in response_text:
        return b
    elif response_text.startswith("A"):
        return a
    elif response_text.startswith("B"):
        return b
    else:
        # If unclear, pick randomly
        print(f"Unclear tournament response: {response_text}, picking randomly")
        return random.choice([a, b])


def call_tournament_model(prompt: str) -> str:
    """Call the configured tournament model."""
    provider = os.getenv("TOURNAMENT_PROVIDER", "google")
    model = os.getenv("TOURNAMENT_MODEL", "gemini-2.5-flash-lite")

    print(f"[tournament] Calling {provider}/{model}")

    if provider == "anthropic":
        client = get_anthropic_client()
        response = client.messages.create(
            model=model,
            max_tokens=10,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    elif provider == "google":
        client = get_google_client()
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_modalities=["TEXT"],
                temperature=0.5,  # Lower temp for more consistent judging
            ),
        )
        return response.candidates[0].content.parts[0].text.strip()
    else:
        raise ValueError(f"Unknown provider: {provider}")


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
