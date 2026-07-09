"""
Subvert pipeline Lambda — the task handler behind the SubvertPipeline state
machine. Takes real news headlines and rewrites them as funny SimCity
2000-style newspaper headlines.

Two LLM stages, each one Anthropic message batch (50% of standard price):

    submit_brainstorm -> [poll] -> submit_generate -> [poll] -> save_headlines

Every task receives {"action": ..., "state": {...}} and returns the full
updated state, which the state machine passes through whole:

    {
      "stories": [{year_month_day, story_id, title, description,
                   entity_hints, random_words}],
      "angles":  [[{angle_name, setup, keywords, brainstorm_model,
                    generate_model}, ...] per story],
      "batch":   {batch_id, status, polls, timed_out}
    }

custom_ids are index-based ("story-3", "gen-3-1") because story ids aren't
guaranteed to satisfy the batch API's custom_id charset, and the stories
array in state is positionally stable for the life of an execution. Request
lists are rebuilt (not stored) when results are processed — see
lib/anthropic_batches.py. The per-story random words are drawn once at
submit time and carried in state so rebuilds produce the same prompts.
"""

import datetime
import hashlib
import json
import os
import random

import boto3
from boto3.dynamodb.conditions import Key
import anthropic
from langfuse import get_client, observe

from lib.anthropic_batches import check_batch_state, resolve_batch, submit_batch
from lib.ssm_secrets import get_secret

from zoneinfo import ZoneInfo

_dynamo_resource = boto3.resource("dynamodb")
_headlines_table = _dynamo_resource.Table("SubvertedHeadlines")
os.environ.setdefault("LANGFUSE_PUBLIC_KEY", get_secret("LANGFUSE_PUBLIC_KEY"))
os.environ.setdefault("LANGFUSE_SECRET_KEY", get_secret("LANGFUSE_SECRET_KEY"))
os.environ.setdefault("LANGFUSE_HOST", "https://us.cloud.langfuse.com")
langfuse = get_client()

# =============================================================================
# MODEL CONFIGURATION
# Set per stage via environment variables. Every call runs through the
# Anthropic Batch API, so models must be Anthropic:
# claude-haiku-4-5, claude-sonnet-5, claude-opus-4-8
# =============================================================================

BRAINSTORM_MODEL = os.getenv("BRAINSTORM_MODEL", "claude-opus-4-8")
GENERATE_MODEL = os.getenv("GENERATE_MODEL", "claude-haiku-4-5-20251001")

# Stage 2 (headline generation) model A/B: one random.choice per angle,
# recorded on each headline as GenerateModel. Currently single-model (100%
# Haiku 4.5) — the Haiku-vs-Sonnet test showed no taste-detectable quality gap
# at 3x the cost. To A/B again, add model IDs back to this list; the per-angle
# selection (made at submit time, carried on the angle in state) and
# GenerateModel tagging stay wired up.
STAGE_2_AB_MODELS = [GENERATE_MODEL]

_anthropic_client = None
_few_shot_cache = None


def get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = anthropic.Anthropic(api_key=get_secret("ANTHROPIC_API_KEY"))
    return _anthropic_client


# Static system prompt for brainstorm stage (>1024 tokens for Anthropic prompt caching)
BRAINSTORM_SYSTEM_PROMPT = """You are a veteran comedy writer brainstorming angles for a satirical newspaper (The Onion meets SimCity 2000). The house voice leans dark — gallows humor, deadpan grimness, and uncomfortable truths land harder than safe punchlines; don't soften the punch to be polite. Given a real headline, find every comedic angle — puns, wordplay, absurdist reframings, dark satire — for a headline writer to develop. Quantity AND quality: each angle needs a real comedic mechanism, not a vague gesture at humor.

ANGLE TYPES (aim for variety):
1. PUN / WORDPLAY — Phonetic puns (must work aloud), double meanings, compound puns. E.g.: "Republicans Can the Jokes Following Death of Rep. Bean" — "can" = stop + preserve in tin.
2. RHYME / ALLITERATION — Musical quality, satisfying meter, natural alliterative runs.
3. ABSURDIST / SURREAL — Logical extremes, mundane framing on extraordinary events, SimCity bureaucratic madness. E.g.: "Local Woman Achieves Elite Frequent Flyer Status Through Emotional Avoidance"
4. DARK SATIRE — Say the uncomfortable truth out loud. Punch UP at power/institutions, never down. E.g.: "ICE to See You: Trump's Immigration Agents Give Ex-Marine Cold Homecoming"
5. CIRCUMLOCUTION — Overly specific roundabout descriptions where the description IS the joke. E.g.: "Dear Abby: Professional Boxer Tired of Getting Hit On at Work"
6. REVERSAL / IRONY — Flip the framing: villain as hero, tragedy as celebration, deadpan wrong conclusions.
7. POP CULTURE REFERENCE — Repurposed titles/catchphrases/lyrics that add meaning, not just recognition.
8. FORMAT-BORROWING / INSTITUTIONAL — Wear the wholesale conventions of an unrelated genre: missing-persons posters (height, last-seen location), product recalls (FDA safety notice, voluntary recall), court rulings (Geneva Convention, international tribunal), weather alerts, scientific journals, AP wire boilerplate. The borrowed format IS the joke. E.g.: 'MISSING: Kevin Durant, 6\'10", last seen in Houston. Lakers defeat search party 112-108' — flyer format applied to a basketball game; the deadpan-realism details and "defeat search party" pivot make it land.

QUALITY BAR:
- Each angle needs a specific mechanism (pun, twist, reference), not just "make it funny"
- Setup must be concrete enough that a writer knows exactly where to go
- Puns must work phonetically, not just visually
- Punch up, not down — satire should be irreverent and afflict the comfortable
- Skip obvious first-draft ideas anyone would think of in 5 seconds

RESPONSE FORMAT:
Return a JSON array with 5 angles, each a DIFFERENT type:
[{"angle_name": "...", "setup": "...", "keywords": ["...", "..."]}]
- angle_name: Type and target (e.g., "pun on 'bill'", "absurdist bureaucracy")
- setup: 1-2 sentences describing the specific comedic premise or mechanism
- keywords: 2-4 specific words/phrases to build the headline around"""


# The curation CLI (Scratch/curate_headlines.py) materializes the editor's
# "outstanding" picks + rationales into this single item — the same source the
# Tournament judge reads. Curated examples lead the few-shot section because
# they carry the editor's taste directly, where judge-ranked picks only
# approximate it.
EXEMPLAR_CACHE_KEY = {'YearMonthDay': 'META', 'HeadlineId': 'outstanding_exemplars'}
MAX_CURATED_FEW_SHOT = 4
MAX_RANKED_FEW_SHOT = 2


def _get_curated_examples() -> list:
    """Editor-curated outstanding headlines (newest first) with rationales."""
    resp = _headlines_table.get_item(Key=EXEMPLAR_CACHE_KEY)
    headlines = (resp.get('Item') or {}).get('Headlines') or []

    examples = []
    for h in headlines[:MAX_CURATED_FEW_SHOT]:
        if not h.get('Headline'):
            continue
        lines = []
        if h.get('OriginalHeadline'):
            lines.append(f"- Original: \"{h['OriginalHeadline']}\"")
            lines.append(f"  Satirical: \"{h['Headline']}\"")
        else:
            lines.append(f"- Satirical: \"{h['Headline']}\"")
        if h.get('Rationale'):
            lines.append(f"  Why it works: {h['Rationale']}")
        examples.append('\n'.join(lines))
    return examples


def _get_recent_top_ranked_examples(skip_headlines: set) -> list:
    """The #1 judge-ranked headline from each of the last few days (freshness)."""
    now = datetime.datetime.now(ZoneInfo('America/New_York'))
    examples = []
    for days_ago in range(1, 5):
        if len(examples) >= MAX_RANKED_FEW_SHOT:
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
            if item.get('Headline', '') in skip_headlines:
                continue
            examples.append(
                f"- Original: \"{item.get('OriginalHeadline', '')}\"\n"
                f"  Satirical: \"{item.get('Headline', '')}\""
            )
    return examples


def get_few_shot_examples() -> str:
    """
    Few-shot style examples for the brainstorm prompt, cached per Lambda warm
    period. Editor-curated outstanding exemplars (with "why it works"
    rationales) lead; recent judge-ranked #1s supplement for freshness.
    """
    global _few_shot_cache
    if _few_shot_cache is not None:
        return _few_shot_cache

    sections = []

    curated = []
    try:
        curated = _get_curated_examples()
        if curated:
            sections.append(
                "\n\nExemplary headlines from our paper (hand-picked by the editor — "
                "match this bar):\n" + "\n".join(curated)
            )
    except Exception as e:
        print(f"Failed to fetch curated exemplars: {e}")

    try:
        curated_titles = {line.split('Satirical: "')[-1].split('"')[0]
                          for line in curated}
        recent = _get_recent_top_ranked_examples(curated_titles)
        if recent:
            sections.append(
                "\n\nRecent top-ranked headlines from our paper:\n" + "\n".join(recent)
            )
    except Exception as e:
        print(f"Failed to fetch few-shot examples: {e}")

    _few_shot_cache = "".join(sections)
    return _few_shot_cache


# ---------------------------------------------------------------------------
# Step Functions task dispatch
# ---------------------------------------------------------------------------

@observe()
def handler(event, context):
    action = event["action"]
    state = event["state"]
    print(f"[subvert-pipeline] action={action}, stories={len(state.get('stories', []))}")
    try:
        if action == "submit_brainstorm":
            return submit_brainstorm(state)
        if action == "check_batch":
            return check_batch_state(get_anthropic_client(), state)
        if action == "submit_generate":
            return submit_generate(state)
        if action == "save_headlines":
            return save_headlines(state)
        raise ValueError(f"Unknown action: {action}")
    finally:
        langfuse.flush()


# ---------------------------------------------------------------------------
# Stage 1: brainstorm comedic angles (one request per story)
# ---------------------------------------------------------------------------

def submit_brainstorm(state: dict) -> dict:
    """Draw each story's random words, then submit one brainstorm batch."""
    stories = [
        {**story, "random_words": get_random_words(8)}
        for story in state["stories"]
    ]
    requests = [_build_brainstorm_request(s, i) for i, s in enumerate(stories)]
    batch = submit_batch(get_anthropic_client(), requests)
    return {**state, "stories": stories, "batch": batch}


def _build_brainstorm_request(story: dict, index: int) -> dict:
    entity_line = ""
    if story.get("entity_hints"):
        entity_line = f"\nReal people, orgs, and topics from the story: {', '.join(story['entity_hints'])}"

    prompt = f"""HEADLINE: "{story['title']}"
CONTEXT: "{story['description']}"{entity_line}

Random words for absurdist friction: {', '.join(story['random_words'])}
Aim to work 2–3 of these in across your angles. Awkward or forced fits are often funnier than natural ones — the juxtaposition is part of the joke. Don't let them swamp the real story.{get_few_shot_examples()}"""

    return {
        "custom_id": f"story-{index}",
        "params": {
            "model": BRAINSTORM_MODEL,
            "max_tokens": 1024,
            "system": [{
                "type": "text",
                "text": BRAINSTORM_SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }],
            "messages": [{"role": "user", "content": prompt}],
        },
    }


# ---------------------------------------------------------------------------
# Stage 2: generate headlines (one request per angle)
# ---------------------------------------------------------------------------

def submit_generate(state: dict) -> dict:
    """Collect brainstorm results, parse angles, submit the generate batch."""
    client = get_anthropic_client()
    stories = state["stories"]
    brainstorm_requests = [_build_brainstorm_request(s, i) for i, s in enumerate(stories)]
    resolved = resolve_batch(client, state["batch"], brainstorm_requests)

    angles_per_story = []
    for i, story in enumerate(stories):
        result = resolved.get(f"story-{i}", {})
        angles = []
        if "text" in result:
            _log_generation(
                model=BRAINSTORM_MODEL,
                params=brainstorm_requests[i]["params"],
                output=result["text"],
                usage=result["usage"],
                metadata={"stage": "brainstorm", "via": result["via"],
                          "story_id": story["story_id"]},
            )
            angles = parse_json_response(result["text"])

        # Ensure we have at least some angles even if the call or parse failed
        if not angles:
            angles = [
                {"angle_name": "wordplay", "setup": "Find puns", "keywords": []},
                {"angle_name": "rhyme", "setup": "Make it rhyme", "keywords": []},
                {"angle_name": "absurd", "setup": "Go weird", "keywords": []},
            ]

        angles = [a for a in angles if isinstance(a, dict)][:5]
        for a in angles:
            a["brainstorm_model"] = BRAINSTORM_MODEL
            a["generate_model"] = random.choice(STAGE_2_AB_MODELS)
        angles_per_story.append(angles)
        print(f"[{story['story_id']}] {len(angles)} angles: {[a.get('angle_name') for a in angles]}")

    generate_requests = _build_generate_requests(stories, angles_per_story)
    batch = submit_batch(client, generate_requests)
    return {**state, "angles": angles_per_story, "batch": batch}


def _build_generate_requests(stories: list, angles_per_story: list) -> list:
    requests = []
    for si, (story, angles) in enumerate(zip(stories, angles_per_story)):
        for ai, angle in enumerate(angles):
            prompt = f"""Write 3-4 funny headlines based on this angle.

ORIGINAL HEADLINE: "{story['title']}"
CONTEXT: "{story['description']}"

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

            requests.append({
                "custom_id": f"gen-{si}-{ai}",
                "params": {
                    "model": angle["generate_model"],
                    "max_tokens": 1024,
                    "messages": [{"role": "user", "content": prompt}],
                },
            })
    return requests


def save_headlines(state: dict) -> dict:
    """Collect generate results and write headlines to SubvertedHeadlines."""
    client = get_anthropic_client()
    stories = state["stories"]
    angles_per_story = state["angles"]
    requests = _build_generate_requests(stories, angles_per_story)
    requests_by_id = {r["custom_id"]: r for r in requests}
    resolved = resolve_batch(client, state["batch"], requests)

    create_time = datetime.datetime.now().isoformat()
    total_saved = 0

    for si, (story, angles) in enumerate(zip(stories, angles_per_story)):
        story_saved = 0
        for ai, angle in enumerate(angles):
            custom_id = f"gen-{si}-{ai}"
            result = resolved.get(custom_id, {})
            if "text" not in result:
                print(f"[{story['story_id']}] No result for angle {ai} "
                      f"({result.get('error', 'missing')}) — skipping")
                continue

            _log_generation(
                model=angle["generate_model"],
                params=requests_by_id[custom_id]["params"],
                output=result["text"],
                usage=result["usage"],
                metadata={"stage": "generate", "via": result["via"],
                          "story_id": story["story_id"],
                          "angle": angle.get("angle_name", "")},
            )

            for hi, parsed in enumerate(parse_json_response(result["text"])):
                if not (isinstance(parsed, dict) and parsed.get("headline")):
                    continue
                _headlines_table.put_item(
                    Item={
                        "YearMonthDay": story["year_month_day"],
                        "HeadlineId": _headline_id(story["story_id"], ai, hi),
                        "CreateTime": create_time,
                        "Headline": parsed["headline"],
                        "Angle": angle.get("angle_name", "unknown"),
                        "AngleSetup": angle.get("setup", ""),
                        "BrainstormModel": angle.get("brainstorm_model", ""),
                        "GenerateModel": angle.get("generate_model", ""),
                        "StoryId": story["story_id"],
                        "OriginalHeadline": story["title"],
                    }
                )
                story_saved += 1

        print(f"Saved {story_saved} headlines for story {story['story_id']} ('{story['title']}')")
        total_saved += story_saved

    return {**state, "saved": total_saved}


def _headline_id(story_id: str, angle_index: int, headline_index: int) -> str:
    """Deterministic per (story, angle, position): a retried pipeline step
    overwrites its earlier writes instead of duplicating them, and the
    overwrite reaches the headline stream as MODIFY, which the tournament
    trigger ignores."""
    seed = f"{story_id}#{angle_index}#{headline_index}"
    return hashlib.sha1(seed.encode()).hexdigest()[:8]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

@observe(as_type="generation")
def _log_generation(model: str, params: dict, output: str, usage: dict, metadata: dict):
    input_messages = []
    if params.get("system"):
        input_messages.append({"role": "system", "content": params["system"][0]["text"]})
    input_messages.extend(params["messages"])
    langfuse.update_current_generation(
        model=model,
        input=input_messages,
        output=output,
        usage_details=usage,
        metadata=metadata,
    )


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


if __name__ == "__main__":
    # Local smoke test: drive the state-machine transitions inline for one
    # test story. Needs .env with ANTHROPIC_API_KEY (+ Langfuse keys) and AWS
    # credentials for DynamoDB. Expect a few minutes of batch-poll waiting.
    import time as _time
    from dotenv import load_dotenv
    load_dotenv()

    def _drive_batch(state):
        while True:
            _time.sleep(30)
            state = check_batch_state(get_anthropic_client(), state)
            if state["batch"]["status"] == "ended":
                return state

    test_state = {
        "stories": [{
            "year_month_day": "TEST",
            "story_id": "test1",
            "title": "Scientists Discover New Species of Deep-Sea Fish",
            "description": "Researchers found the bioluminescent creature at record depths",
            "entity_hints": [],
        }]
    }
    test_state = submit_brainstorm(test_state)
    test_state = _drive_batch(test_state)
    test_state = submit_generate(test_state)
    test_state = _drive_batch(test_state)
    test_state = save_headlines(test_state)
    print(f"Saved {test_state['saved']} headlines (day key TEST)")
    langfuse.flush()
