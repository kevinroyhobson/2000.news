import os
import string
import random
import json
import datetime
import boto3
from boto3.dynamodb.conditions import Key
from concurrent.futures import ThreadPoolExecutor, as_completed
from dynamodb_json import json_util as dynamodb_json
from google import genai
from google.genai import types
import anthropic
from dotenv import load_dotenv
from langfuse import get_client, observe
from lib.ssm_secrets import get_secret

from zoneinfo import ZoneInfo

_dynamo_resource = boto3.resource("dynamodb")
_headlines_table = _dynamo_resource.Table("SubvertedHeadlines")
load_dotenv()
os.environ.setdefault("LANGFUSE_PUBLIC_KEY", get_secret("LANGFUSE_PUBLIC_KEY"))
os.environ.setdefault("LANGFUSE_SECRET_KEY", get_secret("LANGFUSE_SECRET_KEY"))
os.environ.setdefault("LANGFUSE_HOST", "https://us.cloud.langfuse.com")
langfuse = get_client()

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
#   BRAINSTORM_MODEL=claude-opus-4-6
#   GENERATE_PROVIDER=anthropic
#   GENERATE_MODEL=claude-haiku-4-5
# =============================================================================

_anthropic_client = None
_google_client = None
_few_shot_cache = None


# Static system prompt for brainstorm stage (>1024 tokens for Anthropic prompt caching)
BRAINSTORM_SYSTEM_PROMPT = """You are a veteran comedy writer brainstorming angles for a satirical newspaper (The Onion meets SimCity 2000). Given a real headline, find every comedic angle — puns, wordplay, absurdist reframings, dark satire — for a headline writer to develop. Quantity AND quality: each angle needs a real comedic mechanism, not a vague gesture at humor.

ANGLE TYPES (aim for variety):
1. PUN / WORDPLAY — Phonetic puns (must work aloud), double meanings, compound puns. E.g.: "Republicans Can the Jokes Following Death of Rep. Bean" — "can" = stop + preserve in tin.
2. RHYME / ALLITERATION — Musical quality, satisfying meter, natural alliterative runs.
3. ABSURDIST / SURREAL — Logical extremes, mundane framing on extraordinary events, SimCity bureaucratic madness. E.g.: "Local Woman Achieves Elite Frequent Flyer Status Through Emotional Avoidance"
4. DARK SATIRE — Say the uncomfortable truth out loud. Punch UP at power/institutions, never down. E.g.: "ICE to See You: Trump's Immigration Agents Give Ex-Marine Cold Homecoming"
5. CIRCUMLOCUTION — Overly specific roundabout descriptions where the description IS the joke. E.g.: "Dear Abby: Professional Boxer Tired of Getting Hit On at Work"
6. REVERSAL / IRONY — Flip the framing: villain as hero, tragedy as celebration, deadpan wrong conclusions.
7. POP CULTURE REFERENCE — Repurposed titles/catchphrases/lyrics that add meaning, not just recognition.
8. BUREAUCRATIC / INSTITUTIONAL — Human drama as paperwork. "Report Finds" / "Officials Warn" only when the framing IS the joke.

QUALITY BAR:
- Each angle needs a specific mechanism (pun, twist, reference), not just "make it funny"
- Setup must be concrete enough that a writer knows exactly where to go
- Puns must work phonetically, not just visually
- Punch up, not down — satire should be irreverant and afflict the comfortable
- Skip obvious first-draft ideas anyone would think of in 5 seconds

RESPONSE FORMAT:
Return a JSON array with 5 angles, each a DIFFERENT type:
[{"angle_name": "...", "setup": "...", "keywords": ["...", "..."]}]
- angle_name: Type and target (e.g., "pun on 'bill'", "absurdist bureaucracy")
- setup: 1-2 sentences describing the specific comedic premise or mechanism
- keywords: 2-4 specific words/phrases to build the headline around"""


def get_stage_config(stage: str) -> dict:
    """Get provider/model config for a stage from environment variables."""
    stage_upper = stage.upper()
    return {
        "provider": os.getenv(f"{stage_upper}_PROVIDER", "anthropic"),
        "model": os.getenv(f"{stage_upper}_MODEL", "claude-haiku-4-5-20251001"),
    }


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


def get_few_shot_examples() -> str:
    """Fetch recent top-ranked headlines as few-shot style examples. Cached per Lambda warm period."""
    global _few_shot_cache
    if _few_shot_cache is not None:
        return _few_shot_cache

    try:
        now = datetime.datetime.now(ZoneInfo('America/New_York'))
        examples = []
        # Grab the #1 headline from each of the last few days
        for days_ago in range(1, 5):
            if len(examples) >= 2:
                break
            day_key = (now - datetime.timedelta(days=days_ago)).strftime('%Y%m%d')
            response = _headlines_table.query(
                KeyConditionExpression=Key('YearMonthDay').eq(day_key),
            )
            items = response.get('Items', [])
            ranked = [i for i in items if i.get('Rank') is not None]
            ranked.sort(key=lambda x: x['Rank'])
            if ranked:
                item = ranked[0]
                examples.append(
                    f"- Original: \"{item.get('OriginalHeadline', '')}\"\n"
                    f"  Satirical: \"{item.get('Headline', '')}\""
                )

        if examples:
            _few_shot_cache = "\n\nRecent top-ranked headlines from our paper:\n" + "\n".join(examples)
        else:
            _few_shot_cache = ""
    except Exception as e:
        print(f"Failed to fetch few-shot examples: {e}")
        _few_shot_cache = ""

    return _few_shot_cache


def subvert(event, context):
    """Process DynamoDB stream records in parallel."""
    try:
        records_to_process = []

        for record in event["Records"]:
            if record["eventName"] != "INSERT" and record["eventName"] != "MODIFY":
                print(
                    f"Skipped record {record['eventID']} because it's not an INSERT or MODIFY event."
                )
                continue

            story = dynamodb_json.loads(record["dynamodb"]["NewImage"])
            story_id = story.get("StoryId", "")

            # Check if headlines already exist for this story in SubvertedHeadlines
            if story_id and do_headlines_exist_for_story(story["YearMonthDay"], story_id):
                print(f"Skipped {story['Title']} because headlines already exist.")
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
    finally:
        langfuse.flush()


@observe()
def process_story(story):
    """Process a single story - compute subverted titles and save to SubvertedHeadlines."""
    print(f"Starting: {story['Title']}")
    headlines = compute_subverted_titles(story["Title"], story.get("Description", ""))
    save_headlines(story, headlines)
    return {"headline_count": len(headlines)}


def compute_subverted_titles(title: str, subtitle: str):
    """
    Two-stage pipeline:
    1. Brainstorm: Analyze the headline and generate comedic angles + context
    2. Generate: Create polished headlines for each angle
    All candidates are returned for the cross-story tournament to rank.
    """
    print(f"=== STAGE 1: BRAINSTORM ===")
    angles = stage_1_brainstorm(title, subtitle)
    print(f"Generated {len(angles)} angles: {[a['angle_name'] for a in angles]}")

    print(f"=== STAGE 2: GENERATE ===")
    headlines = stage_2_generate(title, subtitle, angles)
    print(f"Generated {len(headlines)} headlines")

    return [format_headline(h) for h in headlines]


@observe()
def stage_1_brainstorm(title: str, subtitle: str) -> list:
    """
    Analyze the headline and brainstorm comedic angles.
    Also does context enrichment: finds pun opportunities, rhymes, references.
    """
    random_words = get_random_words(8)
    few_shot = get_few_shot_examples()

    prompt = f"""HEADLINE: "{title}"
CONTEXT: "{subtitle}"

Random words for inspiration (use if they fit naturally): {', '.join(random_words)}{few_shot}"""

    response_text = call_model("brainstorm", prompt, system_prompt=BRAINSTORM_SYSTEM_PROMPT)
    angles = parse_json_response(response_text)

    # Ensure we have at least some angles even if parsing partially fails
    if not angles or len(angles) == 0:
        angles = [
            {"angle_name": "wordplay", "setup": "Find puns", "keywords": []},
            {"angle_name": "rhyme", "setup": "Make it rhyme", "keywords": []},
            {"angle_name": "absurd", "setup": "Go weird", "keywords": []},
        ]

    return angles[:5]  # Cap at 5 angles


@observe()
def stage_2_generate(title: str, subtitle: str, angles: list) -> list:
    """
    Generate polished headlines for each comedic angle.
    """
    all_headlines = []

    for angle in angles:
        prompt = f"""Write 3-4 funny headlines based on this angle.

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
[{{"headline": "..."}}]"""

        response_text = call_model("generate", prompt)
        parsed = parse_json_response(response_text)

        for h in parsed:
            if isinstance(h, dict) and "headline" in h:
                all_headlines.append({
                    "headline": h["headline"],
                    "angle": angle["angle_name"],
                    "setup": angle["setup"],
                })

    print(f"All generated headlines:")
    for i, h in enumerate(all_headlines, 1):
        print(f"  {i}. [{h['angle']}] {h['headline']}")

    return all_headlines


def format_headline(headline: dict) -> dict:
    """Format a generated headline for storage."""
    return {
        "SubvertedTitle": headline["headline"],
        "Angle": headline.get("angle", "unknown"),
        "AngleSetup": headline.get("setup", ""),
        "SubvertedTitleId": "".join(
            random.choices(string.ascii_lowercase + string.digits, k=5)
        ),
    }


@observe(as_type="generation")
def call_model(stage: str, prompt: str, system_prompt: str = None) -> str:
    """
    Unified model calling interface. Routes to the configured provider/model for each stage.
    """
    config = get_stage_config(stage)
    provider = config["provider"]
    model = config["model"]

    print(f"[{stage}] Calling {provider}/{model}")

    if provider == "anthropic":
        response_text, usage = call_anthropic(model, prompt, system_prompt)
    elif provider == "google":
        response_text, usage = call_google(model, prompt)
    else:
        raise ValueError(f"Unknown provider: {provider}")

    input_messages = [{"role": "user", "content": prompt}]
    if system_prompt:
        input_messages.insert(0, {"role": "system", "content": system_prompt})
    langfuse.update_current_generation(
        model=model,
        input=input_messages,
        output=response_text,
        usage_details=usage,
    )
    return response_text


def call_anthropic(model: str, prompt: str, system_prompt: str = None) -> tuple:
    """Call Anthropic's API with optional cached system prompt. Returns (text, usage_dict)."""
    client = get_anthropic_client()

    kwargs = {
        "model": model,
        "max_tokens": 1024,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system_prompt:
        kwargs["system"] = [{
            "type": "text",
            "text": system_prompt,
            "cache_control": {"type": "ephemeral"},
        }]

    response = client.messages.create(**kwargs)

    usage = {
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
    }
    if hasattr(response.usage, 'cache_creation_input_tokens'):
        usage["cache_creation_input_tokens"] = response.usage.cache_creation_input_tokens
    if hasattr(response.usage, 'cache_read_input_tokens'):
        usage["cache_read_input_tokens"] = response.usage.cache_read_input_tokens
    return response.content[0].text, usage


def call_google(model: str, prompt: str) -> tuple:
    """Call Google's Gemini API. Returns (text, usage_dict)."""
    client = get_google_client()

    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=types.GenerateContentConfig(
            response_modalities=["TEXT"],
            temperature=1.0,
        ),
    )

    usage = {}
    if response.usage_metadata:
        usage = {
            "input_tokens": response.usage_metadata.prompt_token_count,
            "output_tokens": response.usage_metadata.candidates_token_count,
        }
    return response.candidates[0].content.parts[0].text.strip(), usage


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


def save_headlines(story: dict, headlines: list):
    """Save each headline as a separate item in SubvertedHeadlines table."""
    create_time = datetime.datetime.now().isoformat()
    year_month_day = story["YearMonthDay"]
    story_id = story["StoryId"]
    original_headline = story["Title"]

    for headline in headlines:
        _headlines_table.put_item(
            Item={
                "YearMonthDay": year_month_day,
                "HeadlineId": headline["SubvertedTitleId"],
                "CreateTime": create_time,
                "Headline": headline["SubvertedTitle"],
                "Angle": headline.get("Angle", ""),
                "AngleSetup": headline.get("AngleSetup", ""),
                "StoryId": story_id,
                "OriginalHeadline": original_headline,
            }
        )
    print(f"Saved {len(headlines)} headlines for story {story_id}")


def do_headlines_exist_for_story(year_month_day: str, story_id: str) -> bool:
    """Check if any headlines already exist for this story."""
    response = _headlines_table.query(
        KeyConditionExpression=Key("YearMonthDay").eq(year_month_day),
        FilterExpression="StoryId = :sid",
        ExpressionAttributeValues={":sid": story_id},
        Limit=1,
    )
    return len(response.get("Items", [])) > 0


if __name__ == "__main__":
    # Test locally
    results = compute_subverted_titles(
        "Scientists Discover New Species of Deep-Sea Fish",
        "Researchers found the bioluminescent creature at record depths",
    )
    for r in results:
        print(f"  - {r['SubvertedTitle']}")
    langfuse.flush()
