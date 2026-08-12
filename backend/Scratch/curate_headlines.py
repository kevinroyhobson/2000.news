#!/usr/bin/env python3
"""
Curation CLI for marking headlines outstanding/solid/meh/bad.

Pulls ranked, ungraded headlines from SubvertedHeadlines (most recent first by
day, then by in-day rank) and walks one at a time. For 'outstanding' picks,
fires a Claude call to generate a one-sentence rationale, stored alongside the
grade. The 'outstanding' set + rationales become exemplars in the Tournament
system prompt.

The grading itself lives in lib/curation.py, shared with the Telegram reaction
grader — tapping 🏆 on a channel post does what pressing 'o' here does.

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
from boto3.dynamodb.conditions import Key, Attr

# Allow importing lib/ from the parent backend/ directory
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from lib.curation import (  # noqa: E402
    RATIONALE_FALLBACK_MODEL,
    REGION,
    TABLE_NAME,
    apply_grade,
    gen_rationale,
    rebuild_exemplar_cache,
)

GRADE_KEYS = {
    'o': 'outstanding',
    's': 'solid',
    'm': 'meh',
    'b': 'bad',
}


def _interactive_rationale(headline: str, original: str) -> str:
    """Generate a rationale, then offer accept / regenerate / edit / skip."""
    DIM = '\033[2m'
    RED = '\033[31m'
    RESET = '\033[0m'

    rationale = ''
    while True:
        print(f"  {DIM}generating rationale...{RESET}", end='', flush=True)

        def note_refusal(err):
            print(f"\n  {DIM}{err} — falling back to {RATIONALE_FALLBACK_MODEL}...{RESET}",
                  end='', flush=True)

        try:
            rationale = gen_rationale(headline, original, on_refusal=note_refusal)
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
        rationale = ''
        if grade == 'outstanding':
            rationale = _interactive_rationale(h.get('Headline', ''), h.get('OriginalHeadline', ''))

        apply_grade(t, h['YearMonthDay'], h['HeadlineId'], grade,
                    rationale=rationale, source='cli')
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
