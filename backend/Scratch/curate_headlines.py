#!/usr/bin/env python3
"""
Curation CLI for marking headlines outstanding/solid/meh/bad.

Pulls ranked, ungraded headlines from SubvertedHeadlines (most recent first by
day, then by in-day rank) and walks one at a time. For 'outstanding' picks,
fires a Claude call to generate a one-sentence rationale, stored alongside the
grade. The 'outstanding' set + rationales become exemplars in the Tournament
system prompt.

Usage (from backend/ with venv active):
    python3 Scratch/curate_headlines.py [--days 3] [--limit 200]
    python3 Scratch/curate_headlines.py --include-graded   # re-grade already-graded
"""

import argparse
import datetime
import os
import sys
from zoneinfo import ZoneInfo

import boto3
import anthropic
from boto3.dynamodb.conditions import Key, Attr

# Allow importing lib/ from the parent backend/ directory
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from lib.ssm_secrets import get_secret  # noqa: E402


REGION = 'us-east-2'
RATIONALE_MODEL = 'claude-opus-4-7'
RATIONALE_FALLBACK_MODEL = 'claude-sonnet-4-6'
TABLE_NAME = 'SubvertedHeadlines'

# A single synthetic item in SubvertedHeadlines holds the materialized top-20
# "outstanding" exemplars for the Tournament prompt. Keeps Tournament's load
# path to a single GetItem (no scan, no time window) so curation can be sparse.
EXEMPLAR_CACHE_KEY = {'YearMonthDay': 'META', 'HeadlineId': 'outstanding_exemplars'}
# Target total system-prompt tokens after exemplars are appended. Picked to
# clear Opus 4.7's 4,096-token cache threshold with ~20% buffer, while
# staying small enough that the judge isn't drowning in pattern-match anchors.
EXEMPLAR_TOKEN_TARGET = int(os.getenv('EXEMPLAR_TOKEN_TARGET', '5000'))
# Hard upper bound on exemplars cached, regardless of token budget. Floor on
# overfit risk if the rationales are unusually short.
EXEMPLAR_HARD_CAP = 50
TOURNAMENT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Tournament', 'tournament.py')

GRADE_KEYS = {
    'o': 'outstanding',
    's': 'solid',
    'm': 'meh',
    'b': 'bad',
}

RATIONALE_SYSTEM = """You explain why satirical news headlines work, in the style of a comedy editor's brief annotation.

Given a satirical headline (and the original news headline it riffs on), write ONE sentence (<=35 words) explaining why the headline works. Focus on the comic device — wordplay, format-borrowing, literal-reading absurdism, deadpan-institutional framing, surprise misdirection — and what specifically makes it land.

Examples of the style:
- "Hit on" means both flirtation and literally being punched. The advice column framing sells the misdirection — you read it one way, then the other meaning clicks.
- Format-borrowing: missing-persons flyer + sports box score in the same breath. The deadpan-realism details sell the format; the pivot ("defeat search party") is the punchline.

Reply with ONLY the explanation. No preamble, no quotes around it."""


_anthropic_client = None


def get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        api_key = os.getenv('CURATION_ANTHROPIC_API_KEY')
        source = 'env CURATION_ANTHROPIC_API_KEY'
        if not api_key:
            api_key = get_secret('ANTHROPIC_API_KEY')
            source = 'SSM /2000news/ANTHROPIC_API_KEY (production key)'
        print(f"[curate] using API key from: {source}")
        _anthropic_client = anthropic.Anthropic(api_key=api_key)
    return _anthropic_client


class RefusalError(RuntimeError):
    """Raised when the model returns stop_reason=refusal."""


def _gen_rationale_once(headline: str, original: str, model: str) -> str:
    client = get_anthropic_client()
    msg = client.messages.create(
        model=model,
        max_tokens=500,
        system=RATIONALE_SYSTEM,
        messages=[{
            'role': 'user',
            'content': f'SATIRICAL: "{headline}"\nORIGINAL: "{original}"',
        }],
    )
    text_blocks = [b for b in msg.content if getattr(b, 'type', None) == 'text']
    if text_blocks:
        return text_blocks[0].text.strip()
    if msg.stop_reason == 'refusal':
        raise RefusalError(f"{model} refused (likely safety system flagging the topic)")
    block_types = [getattr(b, 'type', '?') for b in msg.content]
    raise RuntimeError(
        f"No text block in response (stop_reason={msg.stop_reason}, "
        f"blocks={block_types}, len={len(msg.content)})"
    )


def gen_rationale(headline: str, original: str) -> str:
    """Generate a rationale, falling back to Sonnet if Opus refuses."""
    try:
        return _gen_rationale_once(headline, original, RATIONALE_MODEL)
    except RefusalError as e:
        DIM = '\033[2m'
        RESET = '\033[0m'
        print(f"\n  {DIM}{e} — falling back to {RATIONALE_FALLBACK_MODEL}...{RESET}", end='', flush=True)
        return _gen_rationale_once(headline, original, RATIONALE_FALLBACK_MODEL)


def _interactive_rationale(headline: str, original: str) -> str:
    """Generate a rationale, then offer accept / regenerate / edit / skip."""
    DIM = '\033[2m'
    RED = '\033[31m'
    RESET = '\033[0m'

    rationale = ''
    while True:
        print(f"  {DIM}generating rationale...{RESET}", end='', flush=True)
        try:
            rationale = gen_rationale(headline, original)
        except Exception as e:
            print(f" {RED}failed: {e}{RESET}")
            try:
                choice = input("    [r]etry  [e]dit manually  [k] skip rationale: ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                return ''
            if choice == 'r':
                continue
            if choice == 'e':
                try:
                    edited = input("    edit: ").strip()
                except (EOFError, KeyboardInterrupt):
                    return ''
                return edited
            return ''
        print(f"\r  {DIM}→ {rationale}{RESET}                    ")
        try:
            choice = input("    [enter] accept  [r]egenerate  [e]dit  [k] skip rationale: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            return rationale
        if choice == '' or choice == 'a':
            return rationale
        if choice == 'r':
            continue
        if choice == 'e':
            try:
                edited = input("    edit: ").strip()
            except (EOFError, KeyboardInterrupt):
                return rationale
            if edited:
                return edited
            return rationale
        if choice == 'k':
            return ''
        print(f"    (enter/r/e/k)")


def _read_static_system_prompt() -> str:
    """Pull the TOURNAMENT_SYSTEM_PROMPT base text from tournament.py source."""
    import re
    src = open(TOURNAMENT_PATH).read()
    m = re.search(r'TOURNAMENT_SYSTEM_PROMPT = """(.*?)"""', src, re.DOTALL)
    if not m:
        raise RuntimeError("Could not locate TOURNAMENT_SYSTEM_PROMPT in tournament.py")
    return m.group(1)


def _build_appendix(headlines: list) -> str:
    if not headlines:
        return ''
    lines = ['', 'ADDITIONAL EXEMPLARS (recent headlines marked outstanding by the editor):', '']
    for h in headlines:
        lines.append(f'- "{h.get("Headline", "")}"')
        if h.get('Rationale'):
            lines.append(f'  Why it works: {h["Rationale"]}')
    return '\n'.join(lines)


def _count_prompt_tokens(client, system_text: str) -> int:
    """Count tokens for a system prompt as Tournament would send it."""
    r = client.messages.count_tokens(
        model='claude-opus-4-7',
        system=[{'type': 'text', 'text': system_text}],
        messages=[{'role': 'user', 'content': 'placeholder'}],
    )
    return r.input_tokens


def rebuild_exemplar_cache(table) -> int:
    """
    Scan SubvertedHeadlines for Grade='outstanding' items, then bin-pack as many
    as fit under the EXEMPLAR_TOKEN_TARGET budget (newest first by GradedAt).
    Writes them as a materialized item Tournament reads at module load.
    Returns count of exemplars written.
    """
    items = []
    kwargs = {
        'FilterExpression': Attr('Grade').eq('outstanding'),
        'ProjectionExpression': '#h, OriginalHeadline, Rationale, GradedAt, #r, CrossDayRank',
        'ExpressionAttributeNames': {'#h': 'Headline', '#r': 'Rank'},
    }
    resp = table.scan(**kwargs)
    items.extend(resp.get('Items', []))
    while 'LastEvaluatedKey' in resp:
        kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']
        resp = table.scan(**kwargs)
        items.extend(resp.get('Items', []))

    items.sort(key=lambda x: x.get('GradedAt', ''), reverse=True)
    candidates = items[:EXEMPLAR_HARD_CAP]

    # Estimate via Anthropic's count_tokens API: compute base size, then how many
    # exemplars we can add before exceeding target.
    static_prompt = _read_static_system_prompt()
    client = get_anthropic_client()
    base_tokens = _count_prompt_tokens(client, static_prompt)

    if base_tokens >= EXEMPLAR_TOKEN_TARGET:
        # Static prompt alone already over budget; cache nothing.
        chosen = []
    elif not candidates:
        chosen = []
    else:
        # One measurement with all candidates → derive avg per exemplar, then
        # pick a count and verify.
        full_appendix = _build_appendix([
            {'Headline': c.get('Headline', ''), 'Rationale': c.get('Rationale', '')}
            for c in candidates
        ])
        full_tokens = _count_prompt_tokens(client, static_prompt + full_appendix)
        added = max(1, full_tokens - base_tokens)
        avg_per = added / len(candidates)
        budget_for_exemplars = EXEMPLAR_TOKEN_TARGET - base_tokens
        target_n = max(1, min(len(candidates), int(budget_for_exemplars / avg_per)))

        # Verify with one more count_tokens; nudge ±1 if we're off
        chosen = candidates[:target_n]
        verify_tokens = _count_prompt_tokens(client, static_prompt + _build_appendix([
            {'Headline': c.get('Headline', ''), 'Rationale': c.get('Rationale', '')}
            for c in chosen
        ]))
        # Trim if over, expand if under and have headroom
        while verify_tokens > EXEMPLAR_TOKEN_TARGET and len(chosen) > 1:
            chosen = chosen[:-1]
            verify_tokens = _count_prompt_tokens(client, static_prompt + _build_appendix([
                {'Headline': c.get('Headline', ''), 'Rationale': c.get('Rationale', '')}
                for c in chosen
            ]))
        while (
            len(chosen) < len(candidates)
            and (verify_tokens + avg_per) <= EXEMPLAR_TOKEN_TARGET
        ):
            chosen = candidates[:len(chosen) + 1]
            verify_tokens = _count_prompt_tokens(client, static_prompt + _build_appendix([
                {'Headline': c.get('Headline', ''), 'Rationale': c.get('Rationale', '')}
                for c in chosen
            ]))
            if verify_tokens > EXEMPLAR_TOKEN_TARGET:
                chosen = chosen[:-1]
                break

    headlines = [
        {
            'Headline': c.get('Headline', ''),
            'OriginalHeadline': c.get('OriginalHeadline', ''),
            'Rationale': c.get('Rationale', ''),
            'GradedAt': c.get('GradedAt', ''),
        }
        for c in chosen
    ]
    final_tokens = base_tokens
    if headlines:
        final_tokens = _count_prompt_tokens(
            client, static_prompt + _build_appendix(headlines)
        )

    table.put_item(Item={
        **EXEMPLAR_CACHE_KEY,
        'Headlines': headlines,
        'UpdatedAt': datetime.datetime.now(ZoneInfo('UTC')).isoformat(),
        'TotalOutstanding': len(items),
        'PromptTokens': final_tokens,
        'TokenTarget': EXEMPLAR_TOKEN_TARGET,
    })
    print(f"  cache: {len(headlines)} exemplars / {len(items)} total outstanding "
          f"→ {final_tokens} tokens (target {EXEMPLAR_TOKEN_TARGET})")
    return len(headlines)


def query_day(table, ymd: str, include_graded: bool) -> list:
    filter_expr = Attr('Rank').exists()
    if not include_graded:
        filter_expr = filter_expr & Attr('Grade').not_exists()
    items = []
    kwargs = {
        'KeyConditionExpression': Key('YearMonthDay').eq(ymd),
        'FilterExpression': filter_expr,
    }
    resp = table.query(**kwargs)
    items.extend(resp.get('Items', []))
    while 'LastEvaluatedKey' in resp:
        kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']
        resp = table.query(**kwargs)
        items.extend(resp.get('Items', []))
    items.sort(key=lambda x: int(x.get('Rank', 999)))
    return items


def run_fill_rationales(days_back: int, limit: int):
    """Walk outstanding-graded headlines that don't have a rationale yet."""
    BOLD = '\033[1m'
    DIM = '\033[2m'
    RESET = '\033[0m'

    dynamo = boto3.resource('dynamodb', region_name=REGION)
    t = dynamo.Table(TABLE_NAME)

    today = datetime.datetime.now(ZoneInfo('America/New_York'))
    items = []
    for d in range(days_back):
        ymd = (today - datetime.timedelta(days=d)).strftime('%Y%m%d')
        kwargs = {
            'KeyConditionExpression': Key('YearMonthDay').eq(ymd),
            'FilterExpression': Attr('Grade').eq('outstanding') & Attr('Rationale').not_exists(),
        }
        resp = t.query(**kwargs)
        items.extend(resp.get('Items', []))
        while 'LastEvaluatedKey' in resp:
            kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']
            resp = t.query(**kwargs)
            items.extend(resp.get('Items', []))
        if len(items) >= limit:
            break
    items = items[:limit]
    print(f"Found {len(items)} outstanding headlines without a rationale.\n")
    if not items:
        return

    filled = 0
    for i, h in enumerate(items):
        print(f"\n{BOLD}── {i+1}/{len(items)} ── [{h['YearMonthDay']}, rank {h.get('Rank','-')}]{RESET}")
        print(f"{BOLD}  {h.get('Headline', '')}{RESET}")
        print(f"  {DIM}orig: {h.get('OriginalHeadline', '')[:140]}{RESET}")
        rationale = _interactive_rationale(h.get('Headline', ''), h.get('OriginalHeadline', ''))
        if not rationale:
            continue
        t.update_item(
            Key={'YearMonthDay': h['YearMonthDay'], 'HeadlineId': h['HeadlineId']},
            UpdateExpression='SET Rationale = :r',
            ExpressionAttributeValues={':r': rationale},
        )
        filled += 1
    if filled > 0:
        try:
            n = rebuild_exemplar_cache(t)
            print(f"\n{DIM}exemplar cache refreshed ({n} entries){RESET}")
        except Exception as e:
            print(f"\n[red]exemplar cache refresh failed: {e}")
    print(f"\nFilled {filled} rationales.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=3, help='days back to consider')
    ap.add_argument('--include-graded', action='store_true',
                    help='also show already-graded headlines (for re-grading)')
    ap.add_argument('--limit', type=int, default=200, help='max items per session')
    ap.add_argument('--fill-rationales', action='store_true',
                    help='walk only outstanding-graded headlines that have no rationale, '
                         'and generate one for each')
    ap.add_argument('--rebuild-cache', action='store_true',
                    help='scan all outstanding-graded headlines and rewrite the materialized '
                         'top-20 cache that Tournament reads. Run once after marking new ones '
                         'manually, or any time the cache may be stale.')
    args = ap.parse_args()

    if args.rebuild_cache:
        dynamo = boto3.resource('dynamodb', region_name=REGION)
        t = dynamo.Table(TABLE_NAME)
        n = rebuild_exemplar_cache(t)
        print(f"Rebuilt exemplar cache: {n} headlines written to META/outstanding_exemplars.")
        return

    if not os.getenv('CURATION_ANTHROPIC_API_KEY'):
        print(
            "\033[33m[curate] hint: CURATION_ANTHROPIC_API_KEY is not set; "
            "will fall back to the production Anthropic key from SSM.\n"
            "         To track curation spend separately, set a dedicated key:\n"
            "         export CURATION_ANTHROPIC_API_KEY=sk-ant-...   "
            "(add to ~/.zshrc to persist)\033[0m\n"
        )

    if args.fill_rationales:
        return run_fill_rationales(args.days, args.limit)

    BOLD = '\033[1m'
    DIM = '\033[2m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    RED = '\033[31m'
    RESET = '\033[0m'
    GRADE_COLOR = {'outstanding': GREEN, 'solid': '', 'meh': YELLOW, 'bad': RED}

    dynamo = boto3.resource('dynamodb', region_name=REGION)
    t = dynamo.Table(TABLE_NAME)

    today = datetime.datetime.now(ZoneInfo('America/New_York'))
    candidates = []
    for d in range(args.days):
        ymd = (today - datetime.timedelta(days=d)).strftime('%Y%m%d')
        candidates.extend(query_day(t, ymd, args.include_graded))
        if len(candidates) >= args.limit:
            break
    candidates = candidates[:args.limit]
    print(f"Pulled {len(candidates)} ranked, ungraded headlines from last {args.days} day(s).\n")

    if not candidates:
        print("Nothing to grade.")
        return

    print("Keys: [o]utstanding  [s]olid  [m]eh  [b]ad  [<enter>] skip  [q] quit")
    print("(outstanding picks generate a Claude rationale, stored with the headline)")

    graded = 0
    for i, h in enumerate(candidates):
        rank = h.get('Rank', '-')
        cdr = h.get('CrossDayRank', '-')
        existing_grade = h.get('Grade')
        existing_marker = f"  {DIM}[currently: {existing_grade}]{RESET}" if existing_grade else ""
        print(f"\n{BOLD}── {i+1}/{len(candidates)} ── [{h['YearMonthDay']}, rank {rank}, cross-day {cdr}]{existing_marker}{RESET}")
        print(f"{BOLD}  {h.get('Headline', '')}{RESET}")
        print(f"  {DIM}orig: {h.get('OriginalHeadline', '')[:140]}{RESET}")

        while True:
            try:
                choice = input("? ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                print(f"\nGraded {graded} this session.")
                return
            if choice == '' or choice in ('o', 's', 'm', 'b', 'q'):
                break
            print("(o/s/m/b/<enter>/q)")
        if choice == 'q':
            print(f"Graded {graded} this session.")
            return
        if choice == '':
            continue

        grade = GRADE_KEYS[choice]
        update_kwargs = {
            'Key': {'YearMonthDay': h['YearMonthDay'], 'HeadlineId': h['HeadlineId']},
            'UpdateExpression': 'SET #g = :g, GradedAt = :ts',
            'ExpressionAttributeNames': {'#g': 'Grade'},
            'ExpressionAttributeValues': {
                ':g': grade,
                ':ts': datetime.datetime.now(ZoneInfo('UTC')).isoformat(),
            },
        }
        rationale = ''
        if grade == 'outstanding':
            rationale = _interactive_rationale(h.get('Headline', ''), h.get('OriginalHeadline', ''))
            if rationale:
                update_kwargs['UpdateExpression'] += ', Rationale = :r'
                update_kwargs['ExpressionAttributeValues'][':r'] = rationale

        t.update_item(**update_kwargs)
        color = GRADE_COLOR.get(grade, '')
        print(f"  {color}saved: {grade}{RESET}")
        graded += 1
        if grade == 'outstanding':
            try:
                n = rebuild_exemplar_cache(t)
                print(f"  {DIM}exemplar cache refreshed ({n} entries){RESET}")
            except Exception as e:
                print(f"  {RED}exemplar cache refresh failed: {e}{RESET}")

    print(f"\nDone. Graded {graded} this session.")


if __name__ == '__main__':
    main()
