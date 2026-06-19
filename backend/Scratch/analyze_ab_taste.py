#!/usr/bin/env python3
"""
Taste-based A/B analysis: do YOU prefer Sonnet 4.6 over Haiku 4.5 for Stage 2?

Reads grades_ab.jsonl (paired-comparison picks from grade_headlines.py), keeps
only cross-model pairs (one Haiku headline vs one Sonnet headline), and reports
how often you picked Sonnet. Compares your Sonnet win-rate to the tournament
judge's within-story 68.9% — the judge is Sonnet 4.5, so a human rate near 69%
corroborates the judge while a rate near 50% points to same-family judge bias.

Records written before grade_headlines.py embedded generate_model are backfilled
from DynamoDB by (YearMonthDay, HeadlineId).

Usage (from backend/ with venv active):
    python3 Scratch/analyze_ab_taste.py [--in Scratch/grades_ab.jsonl]
"""

import argparse
import json
import math
import os

HAIKU = "claude-haiku-4-5-20251001"
SONNET = "claude-sonnet-4-6"
JUDGE_WITHIN_STORY = 0.689  # Sonnet within-story win rate (analyze_ab_judge.py)


def normal_cdf(z):
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def backfill_models(records):
    """Fill missing record[side]['generate_model'] from DynamoDB. Returns count."""
    keymap = {}  # (ymd, hid) -> list of side dicts needing the model
    for r in records:
        for side in ('a', 'b'):
            if not r[side].get('generate_model'):
                keymap.setdefault((r[side]['year_month_day'], r[side]['headline_id']), []).append(r[side])
    if not keymap:
        return 0

    import boto3
    dynamo = boto3.resource('dynamodb', region_name='us-east-2')
    keys = list(keymap.keys())
    fetched = 0
    for i in range(0, len(keys), 100):
        chunk = keys[i:i + 100]
        resp = dynamo.batch_get_item(RequestItems={
            'SubvertedHeadlines': {
                'Keys': [{'YearMonthDay': y, 'HeadlineId': h} for (y, h) in chunk],
                'ProjectionExpression': 'YearMonthDay, HeadlineId, GenerateModel',
            }
        })
        for item in resp['Responses'].get('SubvertedHeadlines', []):
            for ref in keymap.get((item['YearMonthDay'], item['HeadlineId']), []):
                ref['generate_model'] = item.get('GenerateModel', '')
                fetched += 1
    return fetched


def report(rows, label):
    if not rows:
        print(f"=== {label}: no pairs ===\n")
        return
    sonnet_picks = 0
    for r in rows:
        picked = r['a'] if r['pick'] == 'a' else r['b']
        if picked.get('generate_model') == SONNET:
            sonnet_picks += 1
    n = len(rows)
    p = sonnet_picks / n
    se = math.sqrt(0.25 / n)              # SE under H0: p = 0.5
    z = (p - 0.5) / se
    pval = 2 * (1 - normal_cdf(abs(z)))
    sep = math.sqrt(p * (1 - p) / n)      # Wald SE for the CI
    lo, hi = max(0, p - 1.96 * sep), min(1, p + 1.96 * sep)

    print(f"=== {label} (n={n}) ===")
    print(f"  You picked Sonnet {sonnet_picks}/{n} = {p*100:.1f}%  (95% CI {lo*100:.0f}-{hi*100:.0f}%)")
    print(f"  vs 50/50:       z={z:+.2f}, p={pval:.4g}{'  SIGNIFICANT' if pval < 0.05 else '  n.s.'}")

    zj = (p - JUDGE_WITHIN_STORY) / se
    pj = 2 * (1 - normal_cdf(abs(zj)))
    if p <= 0.5:
        read = "you do NOT prefer Sonnet -> the judge's edge looks like same-family bias"
    elif pj < 0.05:
        read = "you prefer Sonnet but weaker than the judge -> part of the judge's edge is likely bias"
    else:
        read = "your preference matches the judge -> the Sonnet edge looks real, not just bias"
    print(f"  vs judge 68.9%: z={zj:+.2f}, p={pj:.4g} -> {read}\n")


def main():
    ap = argparse.ArgumentParser()
    default_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'grades_ab.jsonl')
    ap.add_argument('--in', dest='in_path', default=default_path)
    args = ap.parse_args()

    records = []
    with open(args.in_path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    print(f"Loaded {len(records)} graded pairs from {args.in_path}")

    n_back = backfill_models(records)
    if n_back:
        print(f"Backfilled generate_model for {n_back} headline sides from DynamoDB.")

    cross = [r for r in records
             if {r['a'].get('generate_model'), r['b'].get('generate_model')} == {HAIKU, SONNET}]
    # Only survivor-vs-survivor pairs carry taste signal -- culled-vs-culled is
    # meh-vs-meh. A side is a survivor iff it kept a Rank.
    surv = [r for r in cross
            if r['a'].get('rank') is not None and r['b'].get('rank') is not None]
    culled = len(cross) - len(surv)
    # Same-story = same news on the same day (per-angle A/B -> identical context,
    # differing only in the Stage-2 model). Cross-day same-StoryId doesn't count.
    same_story = [r for r in surv
                  if r['a'].get('story_id')
                  and r['a']['story_id'] == r['b'].get('story_id')
                  and r['a'].get('year_month_day') == r['b'].get('year_month_day')]
    print(f"\n{len(cross)} cross-model pairs; {len(surv)} are survivor-vs-survivor "
          f"(the funny ones), {culled} excluded as culled/meh.")
    print(f"Of the survivor pairs, {len(same_story)} are same-story.\n")

    report(same_story, "SAME-STORY survivors (cleanest, compare to judge 68.9%)")
    report(surv, "ALL survivor cross-model pairs (same + cross story)")

    if len(surv) < 40:
        print(f"NOTE: {len(surv)} survivor pairs is thin for a confident read. "
              f"~{40 - len(surv)} more reaches a usable ~40; 60-80 is solid.")


if __name__ == '__main__':
    main()
