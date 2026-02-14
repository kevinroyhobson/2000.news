"""
Tournament Lambda - Cross-story headline ranking

Triggered by DynamoDB stream on SubvertedHeadlines (batched ~5 min after new headlines).
Ranks all today's headlines via pairwise tournament.
Uses bulk model (Gemini Flash) for early rounds, finals model (Opus) for last 64.

Ranking system:
- Rank 1: Tournament winner
- Rank 2: Lost in final
- Rank 3: Lost in semifinal (2 headlines)
- Rank 4: Lost in quarterfinal (4 headlines)
- And so on for all headlines
"""

import os
import time
import datetime
import random
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from boto3.dynamodb.conditions import Key
from google import genai
from google.genai import types
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
_google_client = None


def get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = anthropic.Anthropic(api_key=get_secret("ANTHROPIC_API_KEY"))
    return _anthropic_client


def get_google_client():
    global _google_client
    if _google_client is None:
        _google_client = genai.Client(api_key=get_secret("GEMINI_API_KEY"))
    return _google_client


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

        # 2. Run elimination tournament - everyone gets a rank
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
    Run full elimination tournament. Every headline gets a rank.
    Uses finals model (Opus) when remaining candidates <= cutoff.

    Returns dict: {rank: [headline_ids]}
    - Rank 1: Winner
    - Rank 2: Lost in final
    - Rank 3: Lost in semifinal
    - etc.
    """
    finals_cutoff = int(os.getenv("TOURNAMENT_FINALS_CUTOFF", "64"))

    # Shuffle to avoid position bias
    random.shuffle(candidates)
    current_round = candidates.copy()

    # Track eliminated headlines by round
    # eliminated[round_num] = [headline_ids eliminated in that round]
    eliminated = {}

    round_num = 1
    while len(current_round) > 1:
        use_finals = len(current_round) <= finals_cutoff
        model_label = "finals" if use_finals else "bulk"
        print(f"--- Tournament Round {round_num} ({len(current_round)} competitors, {model_label}) ---")
        next_round = []
        round_losers = []

        # Build list of pairs for this round
        pairs = []
        for i in range(0, len(current_round) - 1, 2):
            pairs.append((current_round[i], current_round[i + 1]))

        # Process pairs in parallel
        with ThreadPoolExecutor(max_workers=min(50, len(pairs))) as executor:
            future_to_pair = {
                executor.submit(compare_pair, a, b, use_finals=use_finals,
                                round_num=round_num, remaining=len(current_round)): (a, b)
                for a, b in pairs
            }
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


def compare_pair(a: dict, b: dict, use_finals=False,
                  round_num=0, remaining=0) -> dict:
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

    response_text = call_tournament_model(
        prompt, use_finals=use_finals,
        round_num=round_num, remaining=remaining,
    ).strip().upper()

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


MAX_RETRIES = 4
RETRY_BASE_DELAY = 2  # seconds


def call_tournament_model(prompt: str, use_finals=False,
                          round_num=0, remaining=0) -> str:
    """Call the configured tournament model, switching to finals model when appropriate.
    Retries with exponential backoff on rate limit or transient errors.
    Only finals rounds are traced in Langfuse to stay within free tier."""
    if use_finals:
        return _call_tournament_model_traced(
            prompt, round_num=round_num, remaining=remaining,
        )

    return _call_tournament_model_untraceable(
        prompt, round_num=round_num, remaining=remaining,
    )


@observe(as_type="generation")
def _call_tournament_model_traced(prompt: str, round_num=0, remaining=0) -> str:
    """Finals model call — traced in Langfuse."""
    provider = os.getenv("TOURNAMENT_FINALS_PROVIDER", "anthropic")
    model = os.getenv("TOURNAMENT_FINALS_MODEL", "claude-opus-4-6")
    text, usage = _do_api_call(provider, model, round_num, remaining, prompt=prompt)
    langfuse.update_current_generation(
        model=model,
        usage_details=usage,
        metadata={"round_num": round_num, "remaining_count": remaining},
    )
    return text


def _call_tournament_model_untraceable(prompt: str, round_num=0, remaining=0) -> str:
    """Bulk model call — not traced in Langfuse."""
    provider = os.getenv("TOURNAMENT_PROVIDER", "google")
    model = os.getenv("TOURNAMENT_MODEL", "gemini-2.5-flash")
    text, _ = _do_api_call(provider, model, round_num, remaining, prompt=prompt)
    return text


def _do_api_call(provider, model, round_num, remaining, prompt) -> tuple:
    """Make the actual API call with retries. Returns (text, usage_dict)."""
    print(f"[tournament] Calling {provider}/{model} (round={round_num}, remaining={remaining})")

    for attempt in range(MAX_RETRIES + 1):
        try:
            if provider == "anthropic":
                client = get_anthropic_client()
                response = client.messages.create(
                    model=model,
                    max_tokens=10,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = response.content[0].text
                usage = {
                    "input_tokens": response.usage.input_tokens,
                    "output_tokens": response.usage.output_tokens,
                }
            elif provider == "google":
                client = get_google_client()
                response = client.models.generate_content(
                    model=model,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_modalities=["TEXT"],
                        temperature=0.5,
                    ),
                )
                text = response.candidates[0].content.parts[0].text.strip()
                usage = {}
                if response.usage_metadata:
                    usage = {
                        "input_tokens": response.usage_metadata.prompt_token_count,
                        "output_tokens": response.usage_metadata.candidates_token_count,
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
