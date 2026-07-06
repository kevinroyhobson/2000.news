"""
Tournament pipeline Lambda — the task handler behind the TournamentPipeline
state machine. Cross-story headline ranking, progressive: each run ranks only
NEW headlines + top 64 survivors from previous runs.

Each LLM round is one Anthropic message batch (50% of standard price):

    load_candidates -> [ submit_round -> poll -> process_round ]*  (while >20 remain)
                    -> submit_final -> poll -> process_final       (3-judge lensed ensemble)
                    -> load_cross_day -> ...same loop again...     (cross-day tournament)
                    -> finalize                                    (release lock / self-restart)

Every task receives {"action": ..., "state": {...}} and returns the full
updated state. State carries only headline *references* ({"day": ...,
"id": ...}); text is re-read from DynamoDB when prompts are built, keeping
execution state far below the 256KB limit:

    {
      "day": "20260701", "mode": "same_day"|"cross_day", "batch_num": 3,
      "phase": "elimination"|"final"|"skip",
      "groups": [[ref, ...], ...], "final_group": [ref, ...],
      "eliminated_rounds": [[{**ref, "pos": 4}, ...] per round],
      "remaining": 120, "round_num": 2,
      "batch": {batch_id, status, polls, timed_out}
    }

Ranking system:
- Final group (<=20 remain): explicit 1-through-N ordering, ranked once per
  ensemble judge (craft / self-contained / impact lenses) and merged by Borda
  count.
- Mass-cut rounds (same-day rounds whose losers all land below the top-64
  survivor line): the judge picks the best 3 per group, unordered, at high
  effort. Those losers' ranks are discarded, so deliberation goes entirely to
  the cut — the tournament's one irreversible decision. Non-picked headlines
  record a tied position.
- All other elimination rounds: full 1-through-N group ranking at low effort,
  because their losers' positions are published (same-day survivor ranks up
  to 64; every cross-day loser's CrossDayRank drives /today).
"""

import datetime
import json
import os
import random
import uuid
from itertools import groupby
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Key
import anthropic
from langfuse import get_client, observe

from lib import tournament_lock
from lib.anthropic_batches import check_batch_state, resolve_batch, submit_batch
from lib.ssm_secrets import get_secret


_dynamo_resource = boto3.resource('dynamodb')
_headlines_table = _dynamo_resource.Table('SubvertedHeadlines')
_sfn = boto3.client('stepfunctions')
os.environ.setdefault("LANGFUSE_PUBLIC_KEY", get_secret("LANGFUSE_PUBLIC_KEY"))
os.environ.setdefault("LANGFUSE_SECRET_KEY", get_secret("LANGFUSE_SECRET_KEY"))
os.environ.setdefault("LANGFUSE_HOST", "https://us.cloud.langfuse.com")
langfuse = get_client()

SURVIVOR_COUNT = 64
VERBOSE = os.getenv("TOURNAMENT_VERBOSE", "false").lower() == "true"
MODEL_FINAL = os.getenv("TOURNAMENT_MODEL_FINAL", "claude-opus-4-8")
MODEL_ELIMINATION = os.getenv("TOURNAMENT_MODEL_ELIMINATION", "claude-sonnet-5")
# Thinking depth per round type: mass-cut rounds (see _use_pick3) run pick-3
# at high effort because the cut is the only irreversible decision, full-rank
# elimination rounds run low because a coarse ordering is enough, and the
# final runs high. Passed explicitly so behavior is pinned even if the API's
# default effort changes. effort requires Sonnet 4.6+/Opus — remove it before
# pointing MODEL_ELIMINATION at Haiku.
EFFORT_CUT = os.getenv("TOURNAMENT_CUT_EFFORT", "high")
EFFORT_ELIMINATION = os.getenv("TOURNAMENT_ELIMINATION_EFFORT", "low")
EFFORT_FINAL = os.getenv("TOURNAMENT_FINAL_EFFORT", "high")
# Adaptive thinking counts its reasoning tokens against max_tokens, so every
# judge call gets at least this much headroom ahead of the answer line. It's
# a ceiling, not a target — only generated tokens are billed. Raise via env
# if a ranking ever truncates mid-think.
THINKING_MAX_TOKENS_FLOOR = int(os.getenv("TOURNAMENT_MAX_TOKENS_FLOOR", "8000"))

# The final group is ranked once per ensemble judge, each pass led by a
# different lens, and the orderings merge via Borda count. Only the top ~16
# headlines are ever published, so extra deliberation goes exactly where it
# matters. Set to 1 for a single final ranking.
FINAL_ENSEMBLE_SIZE = int(os.getenv("TOURNAMENT_FINAL_ENSEMBLE_SIZE", "3"))

# One lens per ensemble judge. All system-prompt criteria still apply; the
# lens sets which failure mode that judge is least willing to forgive, so the
# panel disagrees in useful ways.
FINAL_ENSEMBLE_LENSES = [
    "Weigh WORDPLAY & CRAFT most heavily: say every pun aloud in your head and "
    "punish any that only work visually; reward tight editing, satisfying meter, "
    "and headlines with a surface reading AND a hidden layer.",
    "Weigh SELF-CONTAINED HUMOR most heavily: apply the forwarding test "
    "ruthlessly — would a stranger who missed today's news laugh with zero "
    "context? Rank down anything that leans on knowing the source story.",
    "Weigh COMEDIC IMPACT most heavily: genuine laughs over smirks, surprise "
    "over recognition, and dark satire that cuts at something real over safe "
    "cleverness. The best satire punches up — it exposes hypocrisy in the "
    "powerful and says the absurdity of how society actually works out loud.",
]

_anthropic_client = None


def get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = anthropic.Anthropic(api_key=get_secret("ANTHROPIC_API_KEY"))
    return _anthropic_client


# ---------------------------------------------------------------------------
# System prompt, cached via cache_control. With the appended exemplars it
# clears every model's minimum cacheable prefix (Opus 4.x: 4096 tokens,
# Sonnet 4.6: 2048). Requests inside one batch process concurrently, so cache
# hits are best-effort; the marker costs nothing on a miss.
# ---------------------------------------------------------------------------
TOURNAMENT_SYSTEM_PROMPT = """You are a veteran comedy editor judging satirical news headlines in the style of The Onion and SimCity 2000's newspaper ticker. Your job is to rank headlines from best to worst based on craft and humor. You have decades of experience in satirical journalism and know exactly what separates a headline that gets a polite chuckle from one that makes coffee come out of someone's nose.

JUDGING CRITERIA:

1. SELF-CONTAINED HUMOR (HARD GATE)
   - Imagine a stranger who has not seen today's news. Reading only the headline, do they laugh?
   - If the joke depends on knowing the specific original news story being parodied, rank it last.
   - The humor must live entirely inside the headline as written. No outside context.
   - "I see what they did there" is not the same as "that's funny." Reward the second, not the first.

2. WORDPLAY & CRAFT
   - Clever alliteration or assonance that flows naturally when read aloud
   - Puns that actually work phonetically, not just visually — say it out loud in your head
   - Unexpected double meanings or semantic twists that reward a second reading
   - Rhythm and meter: headlines should feel punchy, like a good joke setup and punchline
   - Tight editing: every word earns its place, no filler words, no wasted syllables
   - The best headlines have multiple layers — a surface reading AND a hidden meaning

3. COMEDIC IMPACT
   - Does it get a genuine laugh or just a smirk? Rank laughs higher.
   - Surprise factor: does the punchline land where you don't expect it?
   - Dark humor over light — headlines that highlight the absurd nature of the world score higher
   - Satire that cuts: the best headlines make you laugh AND think about something real
   - SimCity 2000 energy: slightly unhinged civic announcements, zany but sharp
   - Deadpan institutional framing applied to absurd subjects — financial agencies downgrading sports teams to junk status, missing-persons alerts for star players, international courts ruling on overtime, scientific announcements about coaches' tantrums. The mismatch between bureaucratic voice and ridiculous content is the SimCity 2000 sweet spot.
   - The "forwarding test": would someone text this to a friend without having to caption it "for context, X happened today"? That's the bar.

4. HEADLINE QUALITY
   - Would this work as an actual newspaper headline? Proper headline grammar matters.
   - Conciseness matters, but density wins over brevity — a long headline that earns every word beats a short one that doesn't.
   - Does it sound like something a real (if slightly unhinged) editor would greenlight?

EXAMPLES OF GREAT HEADLINES (calibrate your taste to this level):

- "Dear Abby: Professional Boxer Tired of Getting Hit On at Work"
  Why it works: "Hit on" means both flirtation and literally being punched. The advice column framing sells the misdirection — you read it one way, then the other meaning clicks. Perfectly self-contained, short, devastating.

- "Churches Partner With Shoe Brands for 'No Sole Left Behind' Voter Registration Blitz"
  Why it works: Two stacked puns (sole/soul AND No Child Left Behind), both phonetically perfect. The headline reads as completely plausible, which makes the puns land harder.

- "Republicans Vow to Can the Jokes Following Death of Rep. Bean"
  Why it works: "Can the jokes" reads straight (stop joking) until the penny drops (canning beans). The double meaning is seamless — you can read it twice and both meanings work.

- '"Grow Up," Screams Man Paid $6M to Watch Other Men Play Basketball'
  Why it works: Pure self-contained absurdism. The comedy lives entirely inside the gap between the demand for maturity and the surreal economics of the speaker's job. You don't need to know who said it, when, or why — the headline is a complete joke. Transferable across any context where it might appear.

- 'MISSING: Kevin Durant, 6\'10", last seen in Houston. Lakers defeat search party 112-108'
  Why it works: Format-borrowing — the headline wears the conventions of a missing-persons flyer (height, last-seen location) and a sports box score in the same breath. The deadpan-realism details ("6'10\"") sell the format; the pivot ("defeat search party") is the punchline that reframes the opponent as failed searchers. Self-contained without knowing who Durant is, that Houston is the Rockets, or what the score was — the headline supplies everything it needs.

- "Local Woman Achieves Elite Frequent Flyer Status Through Emotional Avoidance"
  Why it works: Mundane achievement framing applied to a dark emotional truth. Reads like a lifestyle section piece, hits like a therapy session. Completely self-contained — no news story needed.

EXAMPLES OF MEDIOCRE HEADLINES (things that should rank lower):
- Headlines whose joke depends on knowing the specific news story being parodied — if you stripped away the source story, would this still be funny? If not, rank it down.
- "Inside-baseball" satire that requires being deep in the news cycle to land
- Puns that only work visually on the page, not when spoken aloud — always say it in your head
- Simply making the original headline "wacky" or "random" without a real comedic angle or point
- Meandering headlines that bury the punchline beneath setup — earned length is fine, padding is not
- Obvious first-draft jokes that anyone would think of within 5 seconds of reading the original
- Headlines that are mean-spirited or punch down rather than satirically pointing at absurdity
- Headlines that just add "Area Man" or "Report Finds" without earning the Onion-style framing

RESPONSE FORMAT:
Reply with ALL letters in order from best to worst, separated by commas (e.g., "D, A, F, B, C, E").
Each letter MUST appear exactly once. Do not skip any letters."""


EXEMPLAR_CACHE_KEY = {'YearMonthDay': 'META', 'HeadlineId': 'outstanding_exemplars'}


def _fetch_outstanding_exemplars() -> str:
    """
    Pull cached outstanding-graded headlines from DDB to append to the system prompt.

    Reads a single item (META/outstanding_exemplars) maintained by the curation CLI.
    Runs once at module load; the result becomes part of a stable cached prompt for
    the lifetime of this Lambda warm period.
    """
    try:
        resp = _headlines_table.get_item(Key=EXEMPLAR_CACHE_KEY)
        item = resp.get('Item') or {}
        headlines = item.get('Headlines') or []
        if not headlines:
            return ''
        lines = [
            '',
            'ADDITIONAL EXEMPLARS (recent headlines marked outstanding by the editor):',
            '',
        ]
        for h in headlines:
            lines.append(f'- "{h.get("Headline", "")}"')
            if h.get('Rationale'):
                lines.append(f'  Why it works: {h["Rationale"]}')
        return '\n'.join(lines)
    except Exception as e:
        print(f"[tournament] Failed to load outstanding exemplars: {e}")
        return ''


TOURNAMENT_SYSTEM_PROMPT = TOURNAMENT_SYSTEM_PROMPT + _fetch_outstanding_exemplars()
print(f"[tournament] System prompt length: {len(TOURNAMENT_SYSTEM_PROMPT)} chars")


# ---------------------------------------------------------------------------
# Step Functions task dispatch
# ---------------------------------------------------------------------------

@observe()
def handler(event, context):
    action = event["action"]
    state = event["state"]
    print(f"[tournament-pipeline] action={action}, day={state.get('day')}, "
          f"mode={state.get('mode')}, phase={state.get('phase')}")
    try:
        if action == "load_candidates":
            return load_candidates(state)
        if action == "check_batch":
            return check_batch_state(get_anthropic_client(), state)
        if action == "submit_round":
            return submit_round(state)
        if action == "process_round":
            return process_round(state)
        if action == "submit_final":
            return submit_final(state)
        if action == "process_final":
            return process_final(state)
        if action == "load_cross_day":
            return load_cross_day(state)
        if action == "finalize":
            return finalize(state)
        if action == "abort":
            return abort(state)
        raise ValueError(f"Unknown action: {action}")
    finally:
        langfuse.flush()


# ---------------------------------------------------------------------------
# Candidate loading
# ---------------------------------------------------------------------------

def load_candidates(state: dict) -> dict:
    """Query the day's headlines and stage the same-day tournament."""
    day = state["day"]
    all_headlines = get_headlines_for_day(day)
    print(f"Found {len(all_headlines)} total headlines for {day}")

    new_headlines = [h for h in all_headlines if h.get('tournament_batch') is None]
    previous_survivors = [h for h in all_headlines if h.get('survived') is True]
    print(f"New: {len(new_headlines)}, Previous survivors: {len(previous_survivors)}")

    batch_num = max((h.get('tournament_batch') or 0 for h in all_headlines), default=0) + 1

    if not new_headlines:
        print("No new headlines — skipping tournament")
        return {**state, "phase": "skip", "batch_num": batch_num}

    if len(new_headlines) < 2 and not previous_survivors:
        print("Not enough new headlines for tournament")
        # Ranking the lone headline marks it processed; finalize restarts the
        # pipeline whenever unprocessed headlines remain.
        update_survivors(day, [(1, new_headlines[0]['headline_id'])], batch_num, survived=True)
        return {**state, "phase": "skip", "batch_num": batch_num}

    print(f"Tournament batch #{batch_num}")
    candidates = new_headlines + previous_survivors
    random.shuffle(candidates)
    refs = [{"day": day, "id": h['headline_id']} for h in candidates]
    return {**state, "batch_num": batch_num, **_stage_rounds(refs)}


def _stage_rounds(refs: list) -> dict:
    """Route a shuffled candidate pool into elimination rounds or straight to
    the final (<=20 candidates)."""
    if len(refs) <= 20:
        return {"phase": "final", "final_group": refs, "groups": [],
                "eliminated_rounds": [], "remaining": len(refs), "round_num": 1}
    num_groups = max(1, round(len(refs) / 15))
    return {"phase": "elimination", "groups": distribute_into_groups(refs, num_groups),
            "final_group": [], "eliminated_rounds": [], "remaining": len(refs), "round_num": 1}


# ---------------------------------------------------------------------------
# Elimination rounds — one batch per round, one request per group
# ---------------------------------------------------------------------------

def _use_pick3(state: dict) -> bool:
    """Pick-3 losers all tie at one position, so pick-3 is safe only when
    every loser of the round is guaranteed to stay unpublished. A round's
    3 * num_groups winners all outrank its losers (each winner either reaches
    the final or is eliminated in a later, higher-ranked round), so the losers
    stay below the survivor line iff 3 * num_groups >= SURVIVOR_COUNT.
    Cross-day rounds always full-rank because every pool member's
    CrossDayRank is published."""
    return state["mode"] == "same_day" and 3 * len(state["groups"]) >= SURVIVOR_COUNT


def submit_round(state: dict) -> dict:
    requests = _build_round_requests(state)
    judge_mode = "pick3" if _use_pick3(state) else "full_rank"
    print(f"--- Round {state['round_num']} ({state['remaining']} headlines, "
          f"{len(state['groups'])} groups, mode={state['mode']}, judge={judge_mode}) ---")
    return {**state, "batch": submit_batch(get_anthropic_client(), requests)}


def _build_round_requests(state: dict) -> list:
    lookup = _fetch_headline_lookup([ref for group in state["groups"] for ref in group])
    cross_day = state["mode"] == "cross_day"
    pick3 = _use_pick3(state)
    return [
        _build_ranking_request(
            f"r{state['round_num']}-g{i}",
            [lookup[(ref["day"], ref["id"])] for ref in group],
            remaining=state["remaining"],
            model=MODEL_ELIMINATION,
            effort=EFFORT_CUT if pick3 else EFFORT_ELIMINATION,
            cross_day=cross_day,
            pick3=pick3,
        )
        for i, group in enumerate(state["groups"])
    ]


def process_round(state: dict) -> dict:
    """Collect a round's judgments: top 3 per group advance. In a full-rank
    round the rest record their intra-group finish position for tier
    sub-ranking; in a mass-cut (pick-3) round they tie at position 3."""
    requests = _build_round_requests(state)
    resolved = resolve_batch(get_anthropic_client(), state["batch"], requests)
    lookup = _fetch_headline_lookup([ref for group in state["groups"] for ref in group])
    pick3 = _use_pick3(state)

    winners = []
    round_eliminated = []
    for i, group in enumerate(state["groups"]):
        custom_id = f"r{state['round_num']}-g{i}"
        result = resolved.get(custom_id, {})
        if "text" in result:
            _log_generation(
                model=MODEL_ELIMINATION,
                prompt=requests[i]["params"]["messages"][0]["content"],
                output=result["text"],
                usage=result["usage"],
                metadata={"round_num": state["round_num"], "remaining_count": state["remaining"],
                          "effort": EFFORT_CUT if pick3 else EFFORT_ELIMINATION,
                          "judge_mode": "pick3" if pick3 else "full_rank",
                          "mode": state["mode"], "via": result["via"]},
            )
            if pick3:
                order = _parse_pick3(result["text"], len(group))
                explanation = ""
            else:
                order, explanation = _parse_ranking(result["text"], len(group))
            if VERBOSE and explanation:
                print(f"  Group {i} explanation: {explanation}")
        else:
            print(f"No ranking for group {i} ({result.get('error', 'missing')}) — random order")
            order = list(range(len(group)))
            random.shuffle(order)

        if pick3:
            picked = set(order[:3])
            group_winners = [group[j] for j in order[:3]]
            winners.extend(group_winners)
            # All losers of a pick-3 round tie; _use_pick3 guarantees the tie
            # stays below the published ranks.
            for j, ref in enumerate(group):
                if j not in picked:
                    round_eliminated.append({**ref, "pos": 3})
        else:
            ordered = [group[j] for j in order]
            group_winners = ordered[:3]
            winners.extend(group_winners)
            for pos, ref in enumerate(ordered[3:], start=3):
                round_eliminated.append({**ref, "pos": pos})

        winner_preview = [lookup[(ref["day"], ref["id"])]["headline"][:40] for ref in group_winners]
        print(f"  Group {i} ({len(group)}): winners={winner_preview}")

    eliminated_rounds = state["eliminated_rounds"] + [round_eliminated]
    random.shuffle(winners)

    if len(winners) > 20:
        num_groups = max(1, round(len(winners) / 15))
        return {**state, "phase": "elimination", "eliminated_rounds": eliminated_rounds,
                "groups": distribute_into_groups(winners, num_groups),
                "remaining": len(winners), "round_num": state["round_num"] + 1}

    return {**state, "phase": "final", "eliminated_rounds": eliminated_rounds, "groups": [],
            "final_group": winners, "remaining": len(winners),
            "round_num": state["round_num"] + 1}


# ---------------------------------------------------------------------------
# Final round — lensed ensemble in one batch, merged by Borda count
# ---------------------------------------------------------------------------

def submit_final(state: dict) -> dict:
    print(f"--- Final round ({len(state['final_group'])} headlines, mode={state['mode']}) ---")
    requests = _build_final_requests(state)
    return {**state, "batch": submit_batch(get_anthropic_client(), requests)}


def _build_final_requests(state: dict) -> list:
    group_refs = state["final_group"]
    lookup = _fetch_headline_lookup(group_refs)
    group_data = [lookup[(ref["day"], ref["id"])] for ref in group_refs]
    cross_day = state["mode"] == "cross_day"

    n_judges = min(FINAL_ENSEMBLE_SIZE, len(FINAL_ENSEMBLE_LENSES))
    lenses = FINAL_ENSEMBLE_LENSES[:n_judges] if n_judges > 1 and len(group_refs) >= 3 else [None]

    return [
        _build_ranking_request(
            f"final-{i}", group_data,
            remaining=len(group_refs),
            model=MODEL_FINAL,
            effort=EFFORT_FINAL,
            cross_day=cross_day,
            lens=lens,
        )
        for i, lens in enumerate(lenses)
    ]


def process_final(state: dict) -> dict:
    """Merge the ensemble orderings and write ranks for the whole pool."""
    group = state["final_group"]
    requests = _build_final_requests(state)
    resolved = resolve_batch(get_anthropic_client(), state["batch"], requests)
    lookup = _fetch_headline_lookup(group)

    orderings = []
    for req in requests:
        result = resolved.get(req["custom_id"], {})
        if "text" not in result:
            print(f"No ranking from {req['custom_id']} ({result.get('error', 'missing')})")
            continue
        _log_generation(
            model=MODEL_FINAL,
            prompt=req["params"]["messages"][0]["content"],
            output=result["text"],
            usage=result["usage"],
            metadata={"round_num": state["round_num"], "remaining_count": len(group),
                      "effort": EFFORT_FINAL, "judge_mode": "full_rank",
                      "mode": state["mode"], "via": result["via"]},
        )
        order, _ = _parse_ranking(result["text"], len(group))
        orderings.append([group[j] for j in order])

    if not orderings:
        print("All final judges failed — random final ordering")
        fallback = list(group)
        random.shuffle(fallback)
        orderings = [fallback]

    if len(orderings) > 1:
        judge_picks = [lookup[(o[0]["day"], o[0]["id"])]["headline"][:40] for o in orderings]
        final_ordered = _borda_aggregate(group, orderings)
        consensus = [lookup[(ref["day"], ref["id"])]["headline"][:40] for ref in final_ordered[:3]]
        print(f"  Ensemble #1 picks per judge: {judge_picks}")
        print(f"  Ensemble consensus top 3: {consensus}")
    else:
        final_ordered = orderings[0]

    # Full-pool ranking: final ordering first, then eliminated headlines from
    # later rounds outrank earlier ones, sub-ranked by intra-group position.
    headlines_by_rank = {}
    current_rank = 1
    for ref in final_ordered:
        headlines_by_rank[current_rank] = [ref]
        current_rank += 1
    for round_eliminated in reversed(state["eliminated_rounds"]):
        entries = sorted(round_eliminated, key=lambda x: x["pos"])
        for _pos, pos_group in groupby(entries, key=lambda x: x["pos"]):
            headlines_by_rank[current_rank] = list(pos_group)
            current_rank += 1
    print(f"Final rankings: {len(headlines_by_rank)} distinct ranks (mode={state['mode']})")

    if state["mode"] == "same_day":
        _write_same_day_ranks(state["day"], headlines_by_rank, state["batch_num"])
    else:
        _write_cross_day_ranks(state["day"], headlines_by_rank)
    return state


def _borda_aggregate(group: list, orderings: list) -> list:
    """
    Merge multiple orderings of the same group: lowest summed position wins.
    Ties break by best single-judge position (a headline one judge loved beats
    a uniformly mediocre one), then by the first judge's order for determinism.
    """
    def key(ref):
        return (ref["day"], ref["id"])

    score = {key(ref): 0 for ref in group}
    best = {key(ref): len(group) for ref in group}
    first_pos = {key(ref): i for i, ref in enumerate(orderings[0])}
    for ordering in orderings:
        for pos, ref in enumerate(ordering):
            k = key(ref)
            score[k] += pos
            best[k] = min(best[k], pos)

    by_key = {key(ref): ref for ref in group}
    ranked_keys = sorted(
        score,
        key=lambda k: (score[k], best[k], first_pos.get(k, len(group))),
    )
    return [by_key[k] for k in ranked_keys]


def _write_same_day_ranks(day: str, headlines_by_rank: dict, batch_num: int):
    """Top 64 survive with real ranks, the rest are unranked."""
    survivors = []
    non_survivors = []
    for rank in sorted(headlines_by_rank):
        for ref in headlines_by_rank[rank]:
            if len(survivors) < SURVIVOR_COUNT:
                survivors.append((rank, ref["id"]))
            else:
                non_survivors.append(ref["id"])
    update_survivors(day, survivors, batch_num, survived=True)
    update_non_survivors(day, non_survivors, batch_num, survived=False)


def _write_cross_day_ranks(day: str, headlines_by_rank: dict):
    """Replace CrossDayRank across the 3-day window. Stale ranks are cleared
    only now, at write time, so the previous ordering keeps serving the site
    for the hours the tournament runs."""
    clear_cross_day_ranks([day, get_day_offset(day, -1), get_day_offset(day, -2)])
    updated = 0
    for rank in sorted(headlines_by_rank):
        for ref in headlines_by_rank[rank]:
            _headlines_table.update_item(
                Key={'YearMonthDay': ref["day"], 'HeadlineId': ref["id"]},
                UpdateExpression='SET CrossDayRank = :rank',
                ExpressionAttributeValues={':rank': rank},
            )
            updated += 1
    print(f"Updated CrossDayRank for {updated} headlines")


# ---------------------------------------------------------------------------
# Cross-day tournament — same loop, pool spans 3 days, source story hidden
# ---------------------------------------------------------------------------

def load_cross_day(state: dict) -> dict:
    """Stage the cross-day tournament: today's survivors + top 16 from each of
    the previous two days."""
    day = state["day"]
    yesterday = get_day_offset(day, -1)
    day_before = get_day_offset(day, -2)

    today_pool = [h for h in get_headlines_for_day(day) if h.get('rank') is not None]
    today_pool.sort(key=lambda h: h['rank'])
    today_pool = today_pool[:SURVIVOR_COUNT]
    yesterday_top = get_top_n_for_day(yesterday, 16)
    day_before_top = get_top_n_for_day(day_before, 16)

    refs = ([{"day": day, "id": h['headline_id']} for h in today_pool]
            + [{"day": yesterday, "id": h['headline_id']} for h in yesterday_top]
            + [{"day": day_before, "id": h['headline_id']} for h in day_before_top])
    print(f"Cross-day pool: {len(today_pool)} today + {len(yesterday_top)} yesterday "
          f"+ {len(day_before_top)} day-before = {len(refs)}")

    next_state = {**state, "mode": "cross_day"}
    if len(refs) < 2:
        return {**next_state, "phase": "skip"}
    random.shuffle(refs)
    return {**next_state, **_stage_rounds(refs)}


# ---------------------------------------------------------------------------
# Finalize — restart for headlines that arrived mid-run, or release the lock
# ---------------------------------------------------------------------------

def finalize(state: dict) -> dict:
    """Release the day lock, then start a fresh execution if headlines
    arrived while this run was in flight — their stream events found the lock
    held and exited, leaving this run responsible for them."""
    day = state["day"]
    tournament_lock.release(day)

    new_headlines = [h for h in get_headlines_for_day(day)
                     if h.get('tournament_batch') is None]
    if new_headlines and tournament_lock.acquire(day):
        execution = _sfn.start_execution(
            stateMachineArn=os.environ["TOURNAMENT_STATE_MACHINE_ARN"],
            name=f"{day}-rerun-{uuid.uuid4().hex[:8]}",
            input=json.dumps({"day": day, "mode": "same_day"}),
        )
        print(f"{len(new_headlines)} new headlines arrived mid-run — "
              f"started {execution['executionArn']}")
        return {**state, "rerun": True}

    return {**state, "rerun": False}


def abort(state: dict) -> dict:
    """Catch-path cleanup: release the day lock so the next stream event can
    start a fresh run immediately."""
    day = state.get("day")
    if day:
        tournament_lock.release(day)
        print(f"Released tournament lock for {day} after pipeline failure: "
              f"{state.get('error', {})}")
    return state


# ---------------------------------------------------------------------------
# Ranking request building and parsing
# ---------------------------------------------------------------------------

def _build_ranking_request(custom_id: str, group_data: list, *, remaining: int,
                           model: str, effort: str, cross_day: bool,
                           lens: str = None, pick3: bool = False) -> dict:
    """
    Build one batch request asking the model to judge a group of headlines:
    a full best-to-worst ranking, or (pick3=True) just the 3 best, unordered.

    If cross_day=True, the original news headline is hidden from the judge
    entirely (cross-day readers are far from the news, so headlines must
    stand alone). effort (low/medium/high/max) scales adaptive-thinking depth;
    None/empty takes the API default. lens, if given, is a judging emphasis
    appended to the prompt (ensemble passes).
    """
    labels = [chr(ord('A') + i) for i in range(len(group_data))]

    headline_lines = []
    for label, h in zip(labels, group_data):
        headline_lines.append(f'{label}: "{h["headline"]}"')
        if not cross_day:
            headline_lines.append(
                f'  (Source story for reference only — judge whether the satirical '
                f'headline alone is funny to someone who never saw this: "{h["original_headline"]}")'
            )

    headline_block = '\n'.join(headline_lines)
    valid_max = chr(ord('A') + len(group_data) - 1)

    if pick3:
        explanation_instruction = (
            "\n\nAfter the picks line, on a new line, briefly note what made "
            "your picks stand out (1 sentence)."
        ) if VERBOSE else ""
        max_tokens = 300
        prompt = f"""Pick the 3 best satirical headlines from this group. Only your picks matter — do not rank them against each other, and do not rank the rest.

{headline_block}

OUTPUT FORMAT (strict):
- First line of your response is your picks: exactly 3 letters from A through {valid_max}, comma-separated, in any order. Nothing else on that line.
- Correct: D, A, F
- Incorrect: "The three best are: D, A, F"
- Do NOT add preamble. Do NOT explain unless explicitly asked below. Start your response with a letter.{explanation_instruction}"""
    else:
        is_late = remaining <= 40
        if VERBOSE:
            if is_late:
                explanation_instruction = (
                    "\n\nAfter the ranking line, on a new line, explain your reasoning — "
                    "what made the top picks stand out and what held others back (3-5 sentences)."
                )
            else:
                explanation_instruction = (
                    "\n\nAfter the ranking line, on a new line, briefly note what made "
                    "your top pick stand out (1 sentence)."
                )
            max_tokens = 600 if is_late else 400
        else:
            explanation_instruction = ""
            # Headroom for preamble the parser tolerates ahead of the answer line.
            max_tokens = 500 if is_late else 300

        lens_block = f"\n\nJUDGING EMPHASIS FOR THIS PASS: {lens}" if lens else ""
        prompt = f"""Rank these satirical headlines from best to worst.{lens_block}

{headline_block}

OUTPUT FORMAT (strict):
- First line of your response is the ranking: the letters A through {valid_max} in ranked order, comma-separated. Nothing else on that line.
- Correct: D, A, F, B, C, E
- Incorrect: "Looking at each headline carefully: D, A, F, B, C, E"
- Incorrect: "Here is my ranking: D, A, F, B, C, E"
- Do NOT add preamble. Do NOT explain unless explicitly asked below. Start your response with a letter.{explanation_instruction}"""

    params = {
        "model": model,
        "max_tokens": max(max_tokens, THINKING_MAX_TOKENS_FLOOR),
        "thinking": {"type": "adaptive"},
        "system": [{
            "type": "text",
            "text": TOURNAMENT_SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},
        }],
        "messages": [{"role": "user", "content": prompt}],
    }
    if effort:
        params["output_config"] = {"effort": effort}
    return {"custom_id": custom_id, "params": params}


def _label_index(token: str, group_size: int):
    """Map a group letter label ("A", "B", ...) to its index, or None."""
    if len(token) == 1 and 'A' <= token <= chr(ord('A') + group_size - 1):
        return ord(token) - ord('A')
    return None


def _to_indices(letters: list, group_size: int) -> list:
    """Map letter labels to group indices, dropping non-labels and duplicates."""
    indices = []
    for letter in letters:
        index = _label_index(letter, group_size)
        if index is not None and index not in indices:
            indices.append(index)
    return indices


def _parse_pick3(response_text: str, group_size: int) -> list:
    """
    Map the winning letters of a pick-3 judge response to group indices.
    Missing picks are filled with random unpicked indices; a fully
    unparseable response yields random picks.
    """
    lines = response_text.strip().split('\n')

    # The picks line is comma-separated ("D, A, F"), possibly after preamble.
    letters = []
    for line in lines:
        tokens = [token.strip().upper() for token in line.split(',')]
        if len(_to_indices(tokens, group_size)) >= 2:
            letters = tokens
            break
    if not letters:
        # One letter per line ("D\nA\nF") also counts.
        letters = [line.strip().upper() for line in lines]

    picks = _to_indices(letters, group_size)
    want = min(3, group_size)
    if len(picks) < want:
        print(f"Pick-3 response short {len(picks)}/{want}, filling randomly: "
              f"{response_text[:120]}")
        rest = [i for i in range(group_size) if i not in picks]
        random.shuffle(rest)
        picks.extend(rest[:want - len(picks)])

    return picks[:want]


def _parse_ranking(response_text: str, group_size: int) -> tuple:
    """
    Map the ranked letter line of a judge response to group indices.
    Returns (ordered_indices, explanation). Indices the judge skipped are
    appended in random order; a fully unparseable response degrades to a
    random permutation.
    """
    lines = response_text.strip().split('\n')

    # The ranking line carries at least half the expected letters; anything
    # before it is preamble, anything after it is explanation.
    ranked_letters = []
    explanation_lines = []
    for i, line in enumerate(lines):
        tokens = [token.strip().upper() for token in line.split(',')]
        if not ranked_letters and len(_to_indices(tokens, group_size)) >= group_size // 2:
            ranked_letters = tokens
            explanation_lines = lines[i + 1:]
    explanation = '\n'.join(explanation_lines).strip()

    order = _to_indices(ranked_letters, group_size)
    unmentioned = [i for i in range(group_size) if i not in order]
    random.shuffle(unmentioned)
    order.extend(unmentioned)

    if not ranked_letters:
        print(f"Unparseable ranking response, shuffling: {response_text[:120]}")
        random.shuffle(order)

    return order, explanation


@observe(as_type="generation")
def _log_generation(model: str, prompt: str, output: str, usage: dict, metadata: dict):
    langfuse.update_current_generation(
        model=model,
        input=[
            {"role": "system", "content": TOURNAMENT_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        output=output,
        usage_details=usage,
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------

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
        'original_headline': item.get('OriginalHeadline', ''),
        'tournament_batch': item.get('TournamentBatch'),
        'survived': item.get('Survived'),
        'rank': item.get('Rank'),
    } for item in items]


def _fetch_headline_lookup(refs: list) -> dict:
    """Re-read headline data for a set of refs, keyed by (day, headline_id).
    A headline deleted mid-run gets a stub so group letter positions stay
    aligned between the submit-time and process-time request builds."""
    lookup = {}
    for day in {ref["day"] for ref in refs}:
        for h in get_headlines_for_day(day):
            lookup[(day, h['headline_id'])] = h
    for ref in refs:
        key = (ref["day"], ref["id"])
        if key not in lookup:
            print(f"[tournament] Headline {key} vanished mid-run — using stub")
            lookup[key] = {'headline': '(deleted)', 'headline_id': ref["id"],
                           'original_headline': ''}
    return lookup


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


def clear_cross_day_ranks(day_keys: list):
    """Remove stale CrossDayRank from all headlines in the given days."""
    cleared = 0
    for day_key in day_keys:
        response = _headlines_table.query(
            KeyConditionExpression=Key('YearMonthDay').eq(day_key),
            FilterExpression='attribute_exists(CrossDayRank)',
            ProjectionExpression='YearMonthDay, HeadlineId',
        )
        items = response.get('Items', [])
        while 'LastEvaluatedKey' in response:
            response = _headlines_table.query(
                KeyConditionExpression=Key('YearMonthDay').eq(day_key),
                FilterExpression='attribute_exists(CrossDayRank)',
                ProjectionExpression='YearMonthDay, HeadlineId',
                ExclusiveStartKey=response['LastEvaluatedKey'],
            )
            items.extend(response.get('Items', []))

        for item in items:
            _headlines_table.update_item(
                Key={'YearMonthDay': item['YearMonthDay'], 'HeadlineId': item['HeadlineId']},
                UpdateExpression='REMOVE CrossDayRank',
            )
            cleared += 1
    print(f"Cleared CrossDayRank from {cleared} headlines across {len(day_keys)} days")


if __name__ == "__main__":
    # Local smoke test: drive the state-machine transitions inline for today.
    # Needs .env with ANTHROPIC_API_KEY (+ Langfuse keys) and AWS credentials.
    # Runs the full same-day + cross-day flow — expect batch-poll waiting.
    import time as _time
    from dotenv import load_dotenv
    load_dotenv()

    def _drive_batch(state):
        while True:
            _time.sleep(30)
            state = check_batch_state(get_anthropic_client(), state)
            if state["batch"]["status"] == "ended":
                return state

    def _run(state):
        while state["phase"] == "elimination":
            state = _drive_batch(submit_round(state))
            state = process_round(state)
        if state["phase"] == "final":
            state = _drive_batch(submit_final(state))
            state = process_final(state)
        return state

    day = datetime.datetime.now(ZoneInfo('America/New_York')).strftime('%Y%m%d')
    run_state = load_candidates({"day": day, "mode": "same_day"})
    run_state = _run(run_state)
    if run_state["phase"] != "skip":
        run_state = _run(load_cross_day(run_state))
    # No finalize here: locally we never took the day lock, and finalize would
    # try to start a Step Functions execution.
    print(f"Local tournament run for {day} complete")
    langfuse.flush()
