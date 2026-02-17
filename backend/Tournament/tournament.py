"""
Tournament Lambda - Cross-story headline ranking

Triggered by DynamoDB stream on SubvertedHeadlines (batched ~5 min after new headlines).
Progressive tournament: each run ranks only NEW headlines + top 64 survivors from previous runs.

Uses batch ranking (groups of ~15, rank all, top 3 advance) with all-Opus for quality.
Final run of the day polishes top 16 and runs a cross-day tournament.

Ranking system:
- Final group (<=20 remain): explicit 1-through-N ordering
- Earlier rounds: sub-ranked within tiers by intra-group finish position
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

SURVIVOR_COUNT = 64
VERBOSE = os.getenv("TOURNAMENT_VERBOSE", "false").lower() == "true"
MODEL_FINAL = os.getenv("TOURNAMENT_MODEL_FINAL", "claude-opus-4-6")
MODEL_ELIMINATION = os.getenv("TOURNAMENT_MODEL_ELIMINATION", "claude-sonnet-4-5-20250929")

_anthropic_client = None


def get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = anthropic.Anthropic(api_key=get_secret("ANTHROPIC_API_KEY"))
    return _anthropic_client


# ---------------------------------------------------------------------------
# System prompt for prompt caching (>1024 tokens for Opus cache minimum)
# ---------------------------------------------------------------------------
TOURNAMENT_SYSTEM_PROMPT = """You are a veteran comedy editor judging satirical news headlines in the style of The Onion and SimCity 2000's newspaper ticker. Your job is to rank headlines from best to worst based on craft and humor. You have decades of experience in satirical journalism and know exactly what separates a headline that gets a polite chuckle from one that makes coffee come out of someone's nose.

JUDGING CRITERIA (in order of importance):

1. WORDPLAY & CRAFT
   - Clever alliteration or assonance that flows naturally when read aloud
   - Puns that actually work phonetically, not just visually — say it out loud in your head
   - Unexpected double meanings or semantic twists that reward a second reading
   - Rhythm and meter: headlines should feel punchy, like a good joke setup and punchline
   - Tight editing: every word earns its place, no filler words, no wasted syllables
   - The best headlines have multiple layers — a surface reading AND a hidden meaning

2. COMEDIC IMPACT
   - Does it get a genuine laugh or just a smirk? Rank laughs higher.
   - Surprise factor: does the punchline land where you don't expect it?
   - Dark humor over light — headlines that highlight the absurd nature of the world score higher
   - Satire that cuts: the best headlines make you laugh AND think about something real
   - SimCity 2000 energy: slightly unhinged civic announcements, zany but sharp
   - The "forwarding test": would someone text this to a friend? That's the bar.

3. RELATIONSHIP TO SOURCE MATERIAL
   - How cleverly does the satirical headline play off the original news headline?
   - Does the satire punch up (at power, institutions, absurdity) rather than down?
   - Is the comedic angle fresh and surprising, or is it the obvious first joke anyone would make?
   - Does it transform the original meaning in a way that reveals something true?
   - A great satirical headline makes you see the original story differently.

4. HEADLINE QUALITY
   - Would this work as an actual newspaper headline? Proper headline grammar matters.
   - Is it self-contained? No context should be needed beyond the headline itself.
   - Conciseness: shorter headlines that pack the same punch always beat longer ones.
   - Does it sound like something a real (if slightly unhinged) editor would greenlight?

EXAMPLES OF GREAT HEADLINES (calibrate your taste to this level):

- "Dear Abby: Professional Boxer Tired of Getting Hit On at Work"
  Why it works: "Hit on" means both flirtation and literally being punched. The advice column framing sells the misdirection — you read it one way, then the other meaning clicks. Perfectly self-contained, short, devastating.

- "Churches Partner With Shoe Brands for 'No Sole Left Behind' Voter Registration Blitz"
  Why it works: Two stacked puns (sole/soul AND No Child Left Behind), both phonetically perfect. The headline reads as completely plausible, which makes the puns land harder.

- "Republicans Vow to Can the Jokes Following Death of Rep. Bean"
  Why it works: "Can the jokes" reads straight (stop joking) until the penny drops (canning beans). The double meaning is seamless — you can read it twice and both meanings work.

- "ICE to See You: Trump's Immigration Agents Give Ex-Marine Cold Homecoming"
  Why it works: Pop culture reference (Schwarzenegger's Terminator line) plus sustained cold/ice metaphor. "ICE" is literal (the agency) AND figurative (cold). Every single word pulls double duty.

- "Local Woman Achieves Elite Frequent Flyer Status Through Emotional Avoidance"
  Why it works: Mundane achievement framing applied to a dark emotional truth. Reads like a lifestyle section piece, hits like a therapy session. Completely self-contained — no news story needed.

EXAMPLES OF MEDIOCRE HEADLINES (things that should rank lower):
- Puns that only work visually on the page, not when spoken aloud — always say it in your head
- Simply making the original headline "wacky" or "random" without a real comedic angle or point
- Overly long headlines that meander before getting to the punchline — tighter is better
- Obvious first-draft jokes that anyone would think of within 5 seconds of reading the original
- Headlines that are mean-spirited or punch down rather than satirically pointing at absurdity
- Headlines that just add "Area Man" or "Report Finds" without earning the Onion-style framing

RESPONSE FORMAT:
Reply with ALL letters in order from best to worst, separated by commas (e.g., "D, A, F, B, C, E").
Each letter MUST appear exactly once. Do not skip any letters."""


@observe()
def tournament(event, context):
    """
    Main handler - triggered by DynamoDB stream on SubvertedHeadlines.
    Progressive tournament: ranks new headlines + previous survivors only.
    """
    try:
        today = datetime.datetime.now(ZoneInfo('America/New_York')).strftime('%Y%m%d')
        print(f"Running tournament for {today}")

        all_headlines = get_headlines_for_day(today)
        print(f"Found {len(all_headlines)} total headlines for {today}")

        new_headlines = [h for h in all_headlines if h.get('tournament_batch') is None]
        previous_survivors = [h for h in all_headlines if h.get('survived') is True]
        print(f"New: {len(new_headlines)}, Previous survivors: {len(previous_survivors)}")

        if len(new_headlines) == 0:
            print("No new headlines — skipping tournament")
            return f"No new headlines for {today}, skipping"

        if len(new_headlines) < 2 and not previous_survivors:
            print("Not enough new headlines for tournament")
            if len(new_headlines) == 1:
                update_survivors(today, [(1, new_headlines[0]['headline_id'])], 1, survived=True)
            return f"Only {len(new_headlines)} new headlines for {today}"

        # Determine batch number
        batch_num = max((h.get('tournament_batch') or 0 for h in all_headlines), default=0) + 1
        print(f"Tournament batch #{batch_num}")

        # Tournament: new + previous survivors
        candidates = new_headlines + previous_survivors
        if len(candidates) < 2:
            print("Not enough candidates")
            return f"Only {len(candidates)} candidates"

        headlines_by_rank = run_tournament(candidates)

        # Top 64 survive with real ranks, rest are unranked
        survivors, non_survivors = split_by_rank(headlines_by_rank, SURVIVOR_COUNT)
        update_survivors(today, survivors, batch_num, survived=True)
        update_non_survivors(today, non_survivors, batch_num, survived=False)

        # Cross-day tournament runs every batch to keep rankings fresh
        run_cross_day_tournament(today, survivors)

        # Final run only: polish top 16 headlines
        final_run = is_final_run(today, batch_num)
        if final_run:
            print("Final run of the day — polishing top 16")
            polish_top_headlines(today, survivors[:16])

        return (f"Tournament batch #{batch_num} for {today}: "
                f"{len(candidates)} candidates, {len(survivors)} survivors"
                f"{' (FINAL)' if final_run else ''}")
    finally:
        langfuse.flush()


def get_headlines_for_day(day_key: str) -> list:
    """Query all headlines for a given day from SubvertedHeadlines."""
    response = _headlines_table.query(
        KeyConditionExpression=Key('YearMonthDay').eq(day_key)
    )
    items = response.get('Items', [])

    # Handle pagination for large result sets
    while 'LastEvaluatedKey' in response:
        response = _headlines_table.query(
            KeyConditionExpression=Key('YearMonthDay').eq(day_key),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response.get('Items', []))

    return [{
        'headline': item['Headline'],
        'headline_id': item['HeadlineId'],
        'angle': item.get('Angle', ''),
        'angle_setup': item.get('AngleSetup', ''),
        'original_headline': item.get('OriginalHeadline', ''),
        'story_id': item.get('StoryId', ''),
        'year_month_day': item['YearMonthDay'],
        'tournament_batch': item.get('TournamentBatch'),
        'survived': item.get('Survived'),
        'rank': item.get('Rank'),
        'polished': item.get('OriginalSubverted') is not None,
    } for item in items]


def split_by_rank(headlines_by_rank: dict, survivor_count: int) -> tuple:
    """Split ranked headlines into survivors (top N) and non-survivors."""
    survivors = []
    non_survivors = []

    # headlines_by_rank is {rank_str: [headline_ids]} — flatten in rank order
    all_ranked = []
    for rank_str in sorted(headlines_by_rank.keys(), key=int):
        for hid in headlines_by_rank[rank_str]:
            all_ranked.append((int(rank_str), hid))

    for rank, hid in all_ranked:
        if len(survivors) < survivor_count:
            survivors.append((rank, hid))
        else:
            non_survivors.append(hid)

    return survivors, non_survivors


def update_survivors(day_key: str, survivors: list, batch_num: int, survived: bool):
    """Update top headlines with rank, batch number, and survived flag."""
    for rank, headline_id in survivors:
        _headlines_table.update_item(
            Key={'YearMonthDay': day_key, 'HeadlineId': headline_id},
            UpdateExpression='SET #r = :rank, TournamentBatch = :batch, Survived = :survived',
            ExpressionAttributeNames={'#r': 'Rank'},
            ExpressionAttributeValues={
                ':rank': rank,
                ':batch': batch_num,
                ':survived': survived,
            },
        )
    print(f"Updated {len(survivors)} survivors (batch {batch_num})")


def update_non_survivors(day_key: str, non_survivor_ids: list, batch_num: int, survived: bool):
    """Mark non-survivors: set batch number, survived=False, remove rank."""
    for headline_id in non_survivor_ids:
        _headlines_table.update_item(
            Key={'YearMonthDay': day_key, 'HeadlineId': headline_id},
            UpdateExpression='SET TournamentBatch = :batch, Survived = :survived REMOVE #r',
            ExpressionAttributeNames={'#r': 'Rank'},
            ExpressionAttributeValues={
                ':batch': batch_num,
                ':survived': survived,
            },
        )
    print(f"Updated {len(non_survivor_ids)} non-survivors (batch {batch_num})")


def is_final_run(today: str, batch_num: int) -> bool:
    """Check if this is the final tournament run of the day.
    Final run = 4th batch, or current time is after 9pm ET."""
    if batch_num >= 4:
        return True
    now = datetime.datetime.now(ZoneInfo('America/New_York'))
    return now.hour >= 21


# ---------------------------------------------------------------------------
# Tournament ranking
# ---------------------------------------------------------------------------

def run_tournament(candidates: list) -> dict:
    """
    Run batch ranking tournament. Returns dict: {rank_str: [headline_ids]}.

    Elimination rounds (while remaining > 20):
      Divide into groups of ~15, rank each group, top 3 advance.
    Final round (<=20 remain):
      Rank all in one call -> explicit 1-through-N ordering.
    """
    random.shuffle(candidates)
    remaining = candidates.copy()
    elimination_rounds = []

    round_num = 1
    while len(remaining) > 20:
        num_groups = max(1, round(len(remaining) / 15))
        groups = distribute_into_groups(remaining, num_groups)

        print(f"--- Round {round_num} ({len(remaining)} headlines, {num_groups} groups of ~{len(remaining) // max(1, num_groups)}) ---")

        next_remaining = []
        round_eliminated = []

        with ThreadPoolExecutor(max_workers=min(50, num_groups)) as executor:
            futures = {
                executor.submit(rank_group, group, round_num, len(remaining), MODEL_ELIMINATION): i
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

    # Final round
    print(f"--- Final round ({len(remaining)} headlines) ---")
    final_ordered, explanation = rank_group(remaining, round_num, len(remaining), MODEL_FINAL)

    headlines_by_rank = {}
    current_rank = 1

    for headline in final_ordered:
        headlines_by_rank[str(current_rank)] = [headline['headline_id']]
        current_rank += 1

    for round_eliminated in reversed(elimination_rounds):
        round_eliminated.sort(key=lambda x: x[1])
        for _pos, pos_group in groupby(round_eliminated, key=lambda x: x[1]):
            ids = [hid for hid, _ in pos_group]
            headlines_by_rank[str(current_rank)] = ids
            current_rank += 1

    print(f"Final rankings: {len(headlines_by_rank)} distinct ranks")
    return headlines_by_rank


def distribute_into_groups(items: list, num_groups: int) -> list:
    """Distribute items as evenly as possible into num_groups groups."""
    base_size = len(items) // num_groups
    remainder = len(items) % num_groups

    groups = []
    start = 0
    for i in range(num_groups):
        size = base_size + (1 if i < remainder else 0)
        groups.append(items[start:start + size])
        start += size

    return groups


def rank_group(group: list, round_num: int, remaining: int, model: str = MODEL_FINAL) -> tuple:
    """
    Ask the model to rank a group of headlines from best to worst.
    Returns (ordered_headlines, explanation).
    """
    labels = [chr(ord('A') + i) for i in range(len(group))]

    headline_lines = []
    for label, h in zip(labels, group):
        headline_lines.append(f'{label}: "{h["headline"]}"')
        headline_lines.append(f'  Original: "{h["original_headline"]}"')

    headline_block = '\n'.join(headline_lines)

    is_late = remaining <= 40
    if VERBOSE:
        if is_late:
            explanation_instruction = (
                "\nThen on a new line, explain your reasoning — what made the top picks "
                "stand out and what held others back (3-5 sentences)."
            )
        else:
            explanation_instruction = (
                "\nThen on a new line, briefly note what made your top pick stand out (1 sentence)."
            )
        max_tokens = 400 if is_late else 200
    else:
        explanation_instruction = ""
        max_tokens = 150 if is_late else 100

    prompt = f"""Rank these satirical headlines from best to worst.
Reply ONLY with the letters separated by commas (e.g. "D, A, F, B, C, E"). No preamble.

{headline_block}{explanation_instruction}"""

    response_text = call_tournament_model(
        prompt, max_tokens=max_tokens,
        round_num=round_num, remaining=remaining, model=model,
    )

    # Parse: find the line with comma-separated letters (model often prefixes with preamble)
    lines = response_text.strip().split('\n')
    valid_max = chr(ord('A') + len(group) - 1)

    ranking_line = ''
    valid = []
    explanation_lines = []
    for i, line in enumerate(lines):
        letters = [c.strip().upper() for c in line.split(',')]
        candidates = [l for l in letters if len(l) == 1 and 'A' <= l <= valid_max]
        if len(candidates) >= len(group) // 2 and not valid:
            # This line looks like the ranking — at least half the expected letters
            ranking_line = line
            valid = candidates
            explanation_lines = lines[i+1:]
        elif not valid:
            # Preamble before the ranking line
            continue
        # Lines after ranking line are explanation
    explanation = '\n'.join(explanation_lines).strip()

    label_to_headline = {chr(ord('A') + i): h for i, h in enumerate(group)}

    ordered = []
    seen = set()
    for letter in valid:
        if letter not in seen:
            ordered.append(label_to_headline[letter])
            seen.add(letter)

    unmentioned = [h for i, h in enumerate(group) if chr(ord('A') + i) not in seen]
    if unmentioned:
        random.shuffle(unmentioned)
        ordered.extend(unmentioned)

    if not valid:
        print(f"Unparseable ranking response, shuffling: {ranking_line}")
        random.shuffle(ordered)

    return ordered, explanation


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

MAX_RETRIES = 4
RETRY_BASE_DELAY = 2


@observe(as_type="generation")
def call_tournament_model(prompt: str, max_tokens=200,
                          round_num=0, remaining=0,
                          model: str = MODEL_FINAL) -> str:
    """Call the tournament model with cached system prompt."""
    provider = os.getenv("TOURNAMENT_PROVIDER", "anthropic")
    text, usage = _do_api_call(provider, model, prompt, max_tokens=max_tokens)
    langfuse.update_current_generation(
        model=model,
        input=[
            {"role": "system", "content": TOURNAMENT_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        output=text,
        usage_details=usage,
        metadata={"round_num": round_num, "remaining_count": remaining},
    )
    return text


def _do_api_call(provider: str, model: str, prompt: str,
                 max_tokens: int = 200) -> tuple:
    """Make the actual API call with retries and prompt caching."""
    print(f"[tournament] Calling {provider}/{model} (max_tokens={max_tokens})")

    for attempt in range(MAX_RETRIES + 1):
        try:
            if provider == "anthropic":
                client = get_anthropic_client()
                response = client.messages.create(
                    model=model,
                    max_tokens=max_tokens,
                    system=[{
                        "type": "text",
                        "text": TOURNAMENT_SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }],
                    messages=[{"role": "user", "content": prompt}],
                )
                text = response.content[0].text
                usage = {
                    "input_tokens": response.usage.input_tokens,
                    "output_tokens": response.usage.output_tokens,
                }
                if hasattr(response.usage, 'cache_creation_input_tokens'):
                    usage["cache_creation_input_tokens"] = response.usage.cache_creation_input_tokens
                if hasattr(response.usage, 'cache_read_input_tokens'):
                    usage["cache_read_input_tokens"] = response.usage.cache_read_input_tokens
            else:
                raise ValueError(f"Unknown provider: {provider}")

            return text, usage

        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
            print(f"[tournament] Retry {attempt + 1}/{MAX_RETRIES} after {delay:.1f}s: {e}")
            time.sleep(delay)


# ---------------------------------------------------------------------------
# Polish top headlines
# ---------------------------------------------------------------------------

@observe()
def polish_top_headlines(today: str, survivors: list):
    """Rewrite top 16 headlines for maximum humor. Final run only."""
    if not survivors:
        return

    # survivors is list of (rank, headline_id) — need full headline data
    all_headlines = get_headlines_for_day(today)
    headline_lookup = {h['headline_id']: h for h in all_headlines}

    to_polish = []
    for rank, hid in survivors:
        if hid in headline_lookup and not headline_lookup[hid].get('polished'):
            to_polish.append(headline_lookup[hid])

    if not to_polish:
        return

    print(f"Polishing {len(to_polish)} top headlines")

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {}
        for h in to_polish:
            prompt = f"""Comedy headline editor. Punch up this satirical headline — make it funnier, tighter, or cleverer. Keep the same angle and meaning.

Original news: "{h['original_headline']}"
Satirical version: "{h['headline']}"
Angle: {h.get('angle', '')}

Reply with ONLY the improved headline. If already perfect, return it unchanged."""
            futures[executor.submit(call_tournament_model, prompt, max_tokens=100)] = h

        for future in as_completed(futures):
            h = futures[future]
            try:
                improved = future.result().strip().strip('"')
                if improved and improved != h['headline']:
                    print(f"  Polished: {h['headline'][:40]}... -> {improved[:40]}...")
                    _headlines_table.update_item(
                        Key={'YearMonthDay': h['year_month_day'], 'HeadlineId': h['headline_id']},
                        UpdateExpression='SET Headline = :new, OriginalSubverted = :orig',
                        ExpressionAttributeValues={
                            ':new': improved,
                            ':orig': h['headline'],
                        },
                    )
                else:
                    print(f"  Kept: {h['headline'][:60]}")
            except Exception as e:
                print(f"  Failed to polish {h['headline_id']}: {e}")


# ---------------------------------------------------------------------------
# Cross-day tournament
# ---------------------------------------------------------------------------

@observe()
def run_cross_day_tournament(today: str, today_survivors: list):
    """Rank the best headlines across 3 days."""
    yesterday = get_day_offset(today, -1)
    day_before = get_day_offset(today, -2)

    yesterday_top = get_top_n_for_day(yesterday, 16)
    day_before_top = get_top_n_for_day(day_before, 16)

    # today_survivors is [(rank, hid)] — need full headline objects
    all_today = get_headlines_for_day(today)
    today_lookup = {h['headline_id']: h for h in all_today}
    today_pool = [today_lookup[hid] for _, hid in today_survivors[:64] if hid in today_lookup]

    pool = today_pool + yesterday_top + day_before_top

    # Build headline_id -> day_key lookup for update_cross_day_ranks
    hid_to_day = {h['headline_id']: h['year_month_day'] for h in pool}

    print(f"Cross-day pool: {len(today_pool)} today + {len(yesterday_top)} yesterday + {len(day_before_top)} day-before = {len(pool)}")

    if len(pool) < 2:
        return

    ranked = run_tournament(pool)
    update_cross_day_ranks(ranked, hid_to_day)
    print(f"Cross-day tournament complete: {len(ranked)} ranks assigned")


def get_day_offset(day_key: str, offset: int) -> str:
    """Get day key offset by N days."""
    date = datetime.datetime.strptime(day_key, '%Y%m%d')
    new_date = date + datetime.timedelta(days=offset)
    return new_date.strftime('%Y%m%d')


def get_top_n_for_day(day_key: str, n: int) -> list:
    """Get top N ranked headlines for a given day."""
    all_headlines = get_headlines_for_day(day_key)
    ranked = [h for h in all_headlines if h.get('rank') is not None]
    ranked.sort(key=lambda h: h['rank'])
    return ranked[:n]


def update_cross_day_ranks(headlines_by_rank: dict, hid_to_day: dict):
    """Set CrossDayRank on headlines from the cross-day tournament."""
    updated = 0
    for rank_str, headline_ids in headlines_by_rank.items():
        rank = int(rank_str)
        for headline_id in headline_ids:
            _headlines_table.update_item(
                Key={'YearMonthDay': hid_to_day[headline_id], 'HeadlineId': headline_id},
                UpdateExpression='SET CrossDayRank = :rank',
                ExpressionAttributeValues={':rank': rank},
            )
            updated += 1
    print(f"Updated CrossDayRank for {updated} headlines")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    result = tournament({}, {})
    print(result)
    langfuse.flush()
