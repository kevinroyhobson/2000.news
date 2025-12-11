import os
import string
import random
import json
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from dynamodb_json import json_util as dynamodb_json
from google import genai
from google.genai import types
import anthropic
from dotenv import load_dotenv

_dynamo_resource = boto3.resource("dynamodb")
load_dotenv()

# =============================================================================
# MODEL CONFIGURATION
# Configure via environment variables. Each stage needs a provider and model.
#
# Providers: "anthropic" or "google"
#
# Anthropic models: claude-haiku-4-5, claude-sonnet-4-5, claude-opus-4-5
# Google models: gemini-2.5-flash, gemini-2.5-flash-lite
#
# Example .env:
#   BRAINSTORM_PROVIDER=anthropic
#   BRAINSTORM_MODEL=claude-haiku-4-5
#   GENERATE_PROVIDER=anthropic
#   GENERATE_MODEL=claude-haiku-4-5
#   TOURNAMENT_PROVIDER=google
#   TOURNAMENT_MODEL=gemini-2.5-flash-lite
# =============================================================================

_anthropic_client = None
_google_client = None


def get_stage_config(stage: str) -> dict:
    """Get provider/model config for a stage from environment variables."""
    stage_upper = stage.upper()
    return {
        "provider": os.getenv(f"{stage_upper}_PROVIDER", "anthropic"),
        "model": os.getenv(f"{stage_upper}_MODEL", "claude-haiku-4-5"),
    }


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


def subvert(event, context):
    """Process DynamoDB stream records in parallel."""
    records_to_process = []

    for record in event["Records"]:
        if record["eventName"] != "INSERT" and record["eventName"] != "MODIFY":
            print(
                f"Skipped record {record['eventID']} because it's not an INSERT or MODIFY event."
            )
            continue

        story = dynamodb_json.loads(record["dynamodb"]["NewImage"])
        if "SubvertedTitles" in story and story["SubvertedTitles"] is not None:
            print(f"Skipped {story['Title']} because it has already been subverted.")
            continue

        records_to_process.append(story)

    if not records_to_process:
        print("No records to process.")
        return

    print(f"Processing {len(records_to_process)} stories in parallel...")

    with ThreadPoolExecutor(max_workers=len(records_to_process)) as executor:
        futures = {
            executor.submit(process_story, story): story
            for story in records_to_process
        }
        for future in as_completed(futures):
            story = futures[future]
            try:
                future.result()
                print(f"Completed: {story['Title']}")
            except Exception as e:
                print(f"Failed to process '{story['Title']}': {e}")


def process_story(story):
    """Process a single story - compute subverted titles and save."""
    print(f"Starting: {story['Title']}")
    story["SubvertedTitles"] = compute_subverted_titles(
        story["Title"], story.get("Description", "")
    )
    update_story(story)


def compute_subverted_titles(title: str, subtitle: str):
    """
    Three-stage pipeline:
    1. Brainstorm: Analyze the headline and generate comedic angles + context
    2. Generate: Create polished headlines for each angle
    3. Tournament: Pairwise comparisons to select the best headlines
    """
    print(f"=== STAGE 1: BRAINSTORM ===")
    angles = stage_1_brainstorm(title, subtitle)
    print(f"Generated {len(angles)} angles: {[a['angle_name'] for a in angles]}")

    print(f"=== STAGE 2: GENERATE ===")
    candidates = stage_2_generate(title, subtitle, angles)
    print(f"Generated {len(candidates)} candidate headlines")

    print(f"=== STAGE 3: TOURNAMENT ===")
    winners = stage_3_tournament(candidates, num_winners=4)
    print(f"Selected {len(winners)} winners")

    return winners


def stage_1_brainstorm(title: str, subtitle: str) -> list:
    """
    Analyze the headline and brainstorm comedic angles.
    Also does context enrichment: finds pun opportunities, rhymes, references.
    """
    random_words = get_random_words(8)

    prompt = f"""Analyze this news headline and brainstorm comedic angles for rewriting it.

HEADLINE: "{title}"
CONTEXT: "{subtitle}"

Your task:
1. Identify the key nouns/verbs that could be punned or rhymed
2. Think of pop culture references, memes, wordplay or alliteration/assonance opportunities
3. Generate 4-5 distinct comedic angles to explore

Consider these random words/concepts for inspiration (use if they fit naturally): {', '.join(random_words)}

For each angle, provide:
- angle_name: A short label (e.g., "pun on X", "rhyming", "absurdist")
- setup: The comedic premise or wordplay opportunity
- keywords: Specific words/phrases to incorporate

Return as JSON array:
[{{"angle_name": "...", "setup": "...", "keywords": ["...", "..."]}}]"""

    response_text = call_model("brainstorm", prompt)
    angles = parse_json_response(response_text)

    # Ensure we have at least some angles even if parsing partially fails
    if not angles or len(angles) == 0:
        angles = [
            {"angle_name": "wordplay", "setup": "Find puns", "keywords": []},
            {"angle_name": "rhyme", "setup": "Make it rhyme", "keywords": []},
            {"angle_name": "absurd", "setup": "Go weird", "keywords": []},
        ]

    return angles[:5]  # Cap at 5 angles


def stage_2_generate(title: str, subtitle: str, angles: list) -> list:
    """
    Generate polished headlines for each comedic angle.
    """
    all_candidates = []

    for angle in angles:
        prompt = f"""Write 2-3 funny headlines based on this angle.

ORIGINAL HEADLINE: "{title}"
CONTEXT: "{subtitle}"

COMEDIC ANGLE: {angle['angle_name']}
APPROACH: {angle['setup']}
KEYWORDS TO CONSIDER: {', '.join(angle.get('keywords', []))}

Style guide:
- SimCity 2000 newspaper vibe (zany, pithy, satirical)
- If it's a pun, make sure it actually works phonetically
- If it rhymes, make sure it scans well
- Keep headlines punchy - no periods at the end unless it's multiple sentences
- It's OK to twist the meaning for comedic effect

Return as JSON array:
[{{"headline": "...", "angle_used": "{angle['angle_name']}"}}]"""

        response_text = call_model("generate", prompt)
        headlines = parse_json_response(response_text)

        for h in headlines:
            if isinstance(h, dict) and "headline" in h:
                all_candidates.append({
                    "headline": h["headline"],
                    "angle": angle["angle_name"],
                    "setup": angle["setup"],
                })

    print(f"All generated headlines:")
    for i, c in enumerate(all_candidates, 1):
        print(f"  {i}. [{c['angle']}] {c['headline']}")

    return all_candidates


def stage_3_tournament(candidates: list, num_winners: int = 4) -> list:
    """
    Run pairwise comparisons to select the funniest headlines.
    More reliable than self-scoring.
    """
    if len(candidates) <= num_winners:
        return [format_winner(c) for c in candidates]

    # Shuffle to avoid position bias
    random.shuffle(candidates)
    current_round = candidates.copy()

    # Run elimination rounds until we have our winners
    round_num = 1
    while len(current_round) > num_winners:
        print(f"--- Tournament Round {round_num} ({len(current_round)} competitors) ---")
        next_round = []

        # Process pairs
        for i in range(0, len(current_round) - 1, 2):
            a = current_round[i]
            b = current_round[i + 1]
            winner = compare_pair(a, b)
            next_round.append(winner)
            loser = b if winner == a else a
            print(f"  WINNER: {winner['headline']}")
            print(f"  loser:  {loser['headline']}")
            print()

        # If odd number, last one gets a bye
        if len(current_round) % 2 == 1:
            bye = current_round[-1]
            next_round.append(bye)
            print(f"  BYE: {bye['headline']}")
            print()

        current_round = next_round
        round_num += 1

    return [format_winner(c) for c in current_round]


def compare_pair(a: dict, b: dict) -> dict:
    """
    Ask the model which headline is funnier. Returns the winner.
    """
    prompt = f"""Which headline is better for a satirical newspaper?

Value CRAFT as much as humor:
- Clever alliteration or assonance
- Puns that actually work phonetically
- Unexpected wordplay or double meanings
- Rhythm and flow when read aloud

A straightforward joke that lands is good, but a headline with clever linguistic craft is equally valuable.

A: "{a['headline']}"
B: "{b['headline']}"

Reply with ONLY the letter A or B, nothing else."""

    response_text = call_model("tournament", prompt).strip().upper()

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


def format_winner(candidate: dict) -> dict:
    """Format a winning candidate for storage."""
    return {
        "SubvertedTitle": candidate["headline"],
        "Angle": candidate.get("angle", "unknown"),
        "AngleSetup": candidate.get("setup", ""),
        "SubvertedTitleId": "".join(
            random.choices(string.ascii_lowercase + string.digits, k=5)
        ),
    }


def call_model(stage: str, prompt: str) -> str:
    """
    Unified model calling interface. Routes to the configured provider/model for each stage.
    """
    config = get_stage_config(stage)
    provider = config["provider"]
    model = config["model"]

    print(f"[{stage}] Calling {provider}/{model}")

    if provider == "anthropic":
        return call_anthropic(model, prompt)
    elif provider == "google":
        return call_google(model, prompt)
    else:
        raise ValueError(f"Unknown provider: {provider}")


def call_anthropic(model: str, prompt: str) -> str:
    """Call Anthropic's API."""
    client = get_anthropic_client()

    response = client.messages.create(
        model=model,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )

    return response.content[0].text


def call_google(model: str, prompt: str) -> str:
    """Call Google's Gemini API."""
    client = get_google_client()

    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=types.GenerateContentConfig(
            response_modalities=["TEXT"],
            temperature=1.0,
        ),
    )

    return response.candidates[0].content.parts[0].text.strip()


def parse_json_response(response_text: str) -> list:
    """Parse JSON from model response, handling markdown code blocks."""
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        try:
            # Try to find JSON array in the text
            json_start = response_text.find("[")
            if json_start == -1:
                json_start = response_text.find("{")
            json_end = response_text.rfind("]") + 1
            if json_end == 0:
                json_end = response_text.rfind("}") + 1

            if json_start != -1 and json_end != 0:
                json_str = response_text[json_start:json_end]
                return json.loads(json_str)
            else:
                raise ValueError("No valid JSON found in response")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error parsing response: {e}")
            print(f"Response text: {response_text}")
            return []


def get_random_words(num_words: int):
    """Fetch random words from the Words table for creative inspiration."""
    words_table = _dynamo_resource.Table("Words")
    response = words_table.scan()
    words = response["Items"]
    return random.sample([word["Word"] for word in words], min(num_words, len(words)))


def update_story(story):
    """Update the story in DynamoDB with the subverted titles."""
    stories_table = _dynamo_resource.Table("Stories")
    stories_table.update_item(
        Key={"YearMonthDay": story["YearMonthDay"], "Title": story["Title"]},
        UpdateExpression="set SubvertedTitles = :s",
        ExpressionAttributeValues={":s": story["SubvertedTitles"]},
    )


if __name__ == "__main__":
    # Test locally
    results = compute_subverted_titles(
        "Scientists Discover New Species of Deep-Sea Fish",
        "Researchers found the bioluminescent creature at record depths"
    )
    for r in results:
        print(f"  - {r['SubvertedTitle']}")
