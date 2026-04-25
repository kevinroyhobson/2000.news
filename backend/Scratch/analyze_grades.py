#!/usr/bin/env python3
"""
Analyze paired-comparison grades against the LLM judge's rank ordering.

Reads grades.jsonl (output of grade_headlines.py) and reports:
  - Overall agreement rate between human picks and LLM judge
  - Agreement broken down by pair type (top-vs-mid, within-story, etc.)
  - Headlines the human picked that the judge ranked low (candidates for
    great-headlines exemplars or further prompt iteration)
  - Top-16 headlines the human rejected (candidates for de-prioritization)

Usage:
    python3 Scratch/analyze_grades.py [--in path/to/grades.jsonl]
"""

import argparse
import json
import os
from collections import Counter, defaultdict


def tier(rank):
    if rank is None:
        return 'unranked'
    if rank <= 16: return 'top'
    if rank <= 32: return 'mid_high'
    if rank <= 64: return 'mid_low'
    return 'unranked'


def pair_type(a_rank, b_rank, same_story):
    if same_story:
        return 'within-story'
    ta, tb = tier(a_rank), tier(b_rank)
    return ' vs '.join(sorted([ta, tb]))


def judge_pref(a_rank, b_rank):
    """Which side does the LLM judge prefer? 'a', 'b', or None (tie/can't tell)."""
    if a_rank is None and b_rank is None:
        return None
    if a_rank is None:
        return 'b'
    if b_rank is None:
        return 'a'
    if a_rank < b_rank: return 'a'
    if b_rank < a_rank: return 'b'
    return None


def fmt_pct(n, d):
    return f"{100*n/d:5.1f}%" if d else "  -- "


def main():
    ap = argparse.ArgumentParser()
    default_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'grades.jsonl')
    ap.add_argument('--in', dest='in_path', default=default_path)
    args = ap.parse_args()

    records = []
    with open(args.in_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))

    print(f"Loaded {len(records)} graded pairs from {args.in_path}\n")

    # -- Overall agreement --
    overall_n = 0
    overall_agree = 0
    by_type = defaultdict(lambda: {'n': 0, 'agree': 0})

    upsets = []          # user picked the lower-ranked side
    confirms = []        # user picked the higher-ranked side
    user_picked_unranked = []  # user picked unranked over ranked

    for r in records:
        a, b, pick = r['a'], r['b'], r['pick']
        same_story = a['story_id'] == b['story_id'] and a['story_id']
        pt = pair_type(a['rank'], b['rank'], same_story)
        jp = judge_pref(a['rank'], b['rank'])

        # Skip pairs where judge has no opinion
        if jp is None:
            by_type[pt]['n'] += 0  # noop, just to register key
            continue

        agree = (pick == jp)
        overall_n += 1
        overall_agree += int(agree)
        by_type[pt]['n'] += 1
        by_type[pt]['agree'] += int(agree)

        picked = a if pick == 'a' else b
        rejected = b if pick == 'a' else a

        if not agree:
            upsets.append((picked, rejected, pt))
            if picked['rank'] is None and rejected['rank'] is not None:
                user_picked_unranked.append((picked, rejected))
        else:
            confirms.append((picked, rejected, pt))

    # -- Header --
    print(f"=== OVERALL AGREEMENT ===")
    print(f"User agreed with LLM judge on {overall_agree}/{overall_n} pairs"
          f"  ({fmt_pct(overall_agree, overall_n).strip()})")
    print(f"(Tied/both-unranked pairs excluded from agreement.)\n")

    # -- By pair type --
    print(f"=== AGREEMENT BY PAIR TYPE ===")
    rows = sorted(by_type.items(), key=lambda kv: (-kv[1]['n'], kv[0]))
    print(f"  {'pair type':<35} {'n':>4}  {'agree':>6}  {'rate':>6}")
    for pt, d in rows:
        if d['n'] == 0:
            continue
        print(f"  {pt:<35} {d['n']:>4}  {d['agree']:>6}  {fmt_pct(d['agree'], d['n'])}")

    # -- Headlines the user liked but judge ranked low --
    print(f"\n=== HEADLINES YOU LIKED THAT JUDGE RANKED LOW ===")
    print(f"(Candidates for the great-headlines exemplar list.)\n")
    # Pick from upsets: user picked something lower-ranked
    candidates = [(picked, rejected, pt) for picked, rejected, pt in upsets]
    # Sort by how big the gap is (unranked picks first, then by rank delta)
    def gap(picked, rejected):
        pr = picked['rank'] if picked['rank'] is not None else 999
        rr = rejected['rank'] if rejected['rank'] is not None else 999
        return pr - rr
    candidates.sort(key=lambda x: -gap(x[0], x[1]))
    for picked, rejected, pt in candidates[:15]:
        pr = picked['rank'] or 'unrank'
        rr = rejected['rank'] or 'unrank'
        print(f"  YOUR PICK (rank {pr}):  {picked['headline']}")
        print(f"  over    (rank {rr}):  {rejected['headline']}")
        print(f"  type: {pt}\n")

    # -- Top-16 headlines the user rejected --
    print(f"\n=== TOP-16 HEADLINES YOU REJECTED ===")
    print(f"(Candidates for prompt anti-examples or de-prioritization.)\n")
    rejected_top = [(picked, rejected, pt) for picked, rejected, pt in upsets
                    if rejected['rank'] is not None and rejected['rank'] <= 16]
    # Dedupe by rejected headline_id, keep best example
    seen = set()
    unique_rejected = []
    for picked, rejected, pt in rejected_top:
        if rejected['headline_id'] in seen:
            continue
        seen.add(rejected['headline_id'])
        unique_rejected.append((picked, rejected, pt))

    for picked, rejected, pt in unique_rejected[:15]:
        pr = picked['rank'] or 'unrank'
        print(f"  REJECTED (rank {rejected['rank']}):  {rejected['headline']}")
        print(f"  in favor of (rank {pr}):  {picked['headline']}\n")

    # -- Summary stats --
    print(f"\n=== SAMPLE COMPOSITION ===")
    type_counts = Counter()
    for r in records:
        same_story = r['a']['story_id'] == r['b']['story_id'] and r['a']['story_id']
        type_counts[pair_type(r['a']['rank'], r['b']['rank'], same_story)] += 1
    for pt, c in type_counts.most_common():
        print(f"  {pt:<35} {c:>4}")

    # -- Pick distribution --
    pick_counts = Counter(r['pick'] for r in records)
    print(f"\n=== PICK DISTRIBUTION ===")
    print(f"  picked A: {pick_counts['a']}")
    print(f"  picked B: {pick_counts['b']}")
    print(f"  (positions were randomized per pair, so big skew → potential bias)")


if __name__ == '__main__':
    main()
