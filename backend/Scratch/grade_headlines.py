#!/usr/bin/env python3
"""
Paired-comparison grading CLI for satirical headlines.

Pulls a stratified sample from SubvertedHeadlines and walks you through A/B
comparisons. Saves picks to a JSONL file; resume by re-running. Use the
collected picks to evaluate whether the LLM judge's rank ordering agrees
with your taste — and to measure the effect of future prompt changes.

Usage (from backend/ with venv active):
    python3 Scratch/grade_headlines.py --target 150 --days 5
    python3 Scratch/grade_headlines.py --show-sources    # if you want to see originals

Pair distribution is stratified across rank tiers:
    top (rank 1-16)   mid_high (17-32)   mid_low (33-64)   unranked
"""

import argparse
import datetime
import json
import os
import random
import sys
from collections import defaultdict
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Key


def all_query(tbl, **kw):
    items = []
    r = tbl.query(**kw)
    items.extend(r.get('Items', []))
    while 'LastEvaluatedKey' in r:
        r = tbl.query(ExclusiveStartKey=r['LastEvaluatedKey'], **kw)
        items.extend(r.get('Items', []))
    return items


def fetch_headlines(days_back: int) -> list:
    dynamo = boto3.resource('dynamodb', region_name='us-east-2')
    t = dynamo.Table('SubvertedHeadlines')
    today = datetime.datetime.now(ZoneInfo('America/New_York'))
    out = []
    for d in range(days_back):
        ymd = (today - datetime.timedelta(days=d)).strftime('%Y%m%d')
        out.extend(all_query(t, KeyConditionExpression=Key('YearMonthDay').eq(ymd)))
    return out


def tier_for(item) -> str:
    r = item.get('Rank')
    if r is None:
        return 'unranked'
    r = int(r)
    if r <= 16: return 'top'
    if r <= 32: return 'mid_high'
    if r <= 64: return 'mid_low'
    return 'unranked'


def make_pairs(headlines: list, target: int) -> list:
    by_tier = defaultdict(list)
    by_story = defaultdict(list)
    for h in headlines:
        by_tier[tier_for(h)].append(h)
        by_story[h.get('StoryId', '')].append(h)

    pairs = []
    seen_keys = set()

    def add(a, b):
        if a['HeadlineId'] == b['HeadlineId']:
            return
        key = frozenset({a['HeadlineId'], b['HeadlineId']})
        if key in seen_keys:
            return
        seen_keys.add(key)
        pairs.append((a, b))

    def pick_pairs(t1, t2, n):
        attempts = 0
        added = 0
        while added < n and attempts < n * 10:
            attempts += 1
            if not by_tier.get(t1) or not by_tier.get(t2):
                return
            a = random.choice(by_tier[t1])
            b = random.choice(by_tier[t2])
            before = len(pairs)
            add(a, b)
            if len(pairs) > before:
                added += 1

    # Distribution: emphasize tier-boundary calibration over easy wins
    pick_pairs('top', 'mid_high', round(target * 0.30))     # is the rank cutoff meaningful?
    pick_pairs('top', 'mid_low',  round(target * 0.15))
    pick_pairs('top', 'unranked', round(target * 0.20))     # gross sanity check
    pick_pairs('top', 'top',      round(target * 0.15))     # fine-grained taste
    pick_pairs('mid_high', 'mid_low', round(target * 0.10))

    # Within-story pairs (pure craft test — both about the same news)
    story_groups = [s for s in by_story.values() if len(s) >= 2]
    target_within = round(target * 0.10)
    attempts = 0
    while sum(1 for p in pairs if p[0].get('StoryId') == p[1].get('StoryId')) < target_within \
          and attempts < target_within * 10 and story_groups:
        attempts += 1
        group = random.choice(story_groups)
        a, b = random.sample(group, 2)
        add(a, b)

    random.shuffle(pairs)
    return pairs


def load_existing(path: str) -> set:
    if not os.path.exists(path):
        return set()
    seen = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                seen.add(frozenset({rec['a']['headline_id'], rec['b']['headline_id']}))
            except Exception:
                continue
    return seen


def to_record(h: dict) -> dict:
    return {
        'headline_id': h['HeadlineId'],
        'year_month_day': h['YearMonthDay'],
        'headline': h.get('Headline', ''),
        'original_headline': h.get('OriginalHeadline', ''),
        'rank': int(h['Rank']) if h.get('Rank') is not None else None,
        'cross_day_rank': int(h['CrossDayRank']) if h.get('CrossDayRank') is not None else None,
        'story_id': h.get('StoryId', ''),
    }


def render(idx: int, total: int, left: dict, right: dict, show_sources: bool):
    DIM = '\033[2m'
    BOLD = '\033[1m'
    RESET = '\033[0m'
    print()
    print(f"{BOLD}── pair {idx + 1}/{total} ──{RESET}")
    print()
    print(f"{BOLD}A:{RESET} {left.get('Headline', '')}")
    if show_sources:
        print(f"   {DIM}orig: {left.get('OriginalHeadline', '')[:120]}{RESET}")
    print()
    print(f"{BOLD}B:{RESET} {right.get('Headline', '')}")
    if show_sources:
        print(f"   {DIM}orig: {right.get('OriginalHeadline', '')[:120]}{RESET}")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--target', type=int, default=150, help='target number of pairs to generate')
    ap.add_argument('--days', type=int, default=5, help='days of headlines to sample from')
    ap.add_argument('--out', default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'grades.jsonl'))
    ap.add_argument('--show-sources', action='store_true',
                    help='show original headlines (default hidden, matching cross-day judging)')
    args = ap.parse_args()

    print(f"Loading headlines from last {args.days} days...")
    headlines = fetch_headlines(args.days)
    print(f"Loaded {len(headlines)} headlines.")

    pairs = make_pairs(headlines, args.target)
    print(f"Generated {len(pairs)} candidate pairs.")

    seen = load_existing(args.out)
    pairs = [p for p in pairs if frozenset({p[0]['HeadlineId'], p[1]['HeadlineId']}) not in seen]
    print(f"{len(pairs)} new pairs to grade ({len(seen)} already graded).")
    print(f"Saving to: {args.out}")
    print()
    print("[a] left funnier   [b] right funnier   [s] skip   [q] save & quit")
    print("(positions are randomized per pair to remove ordering bias)")

    graded = 0
    with open(args.out, 'a') as f:
        for i, (a, b) in enumerate(pairs):
            # Randomize display position to avoid systematic A-side bias
            if random.random() < 0.5:
                left, right = a, b
                left_label, right_label = 'a', 'b'
            else:
                left, right = b, a
                left_label, right_label = 'b', 'a'

            render(i, len(pairs), left, right, args.show_sources)

            while True:
                try:
                    choice = input("? ").strip().lower()
                except (EOFError, KeyboardInterrupt):
                    print("\nSaving & quitting.")
                    print(f"Graded {graded} pairs this session.")
                    return
                if choice in ('a', 'b', 's', 'q'):
                    break
                print("(a/b/s/q only)")

            if choice == 'q':
                print(f"Saved. Graded {graded} pairs this session.")
                return
            if choice == 's':
                continue

            # Map display-side choice back to underlying a/b
            actual_pick = left_label if choice == 'a' else right_label
            rec = {
                'timestamp': datetime.datetime.now().isoformat(),
                'a': to_record(a),
                'b': to_record(b),
                'pick': actual_pick,
            }
            f.write(json.dumps(rec) + '\n')
            f.flush()
            graded += 1

    print(f"Done. Graded {graded} pairs this session.")


if __name__ == '__main__':
    main()
