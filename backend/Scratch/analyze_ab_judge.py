#!/usr/bin/env python3
"""
Judge-based A/B analysis: Haiku 4.5 vs Sonnet 4.6 for Stage 2 generation.

No human grading required. Uses the tournament judge's own verdict, which is
already recorded on every headline:
  - Survived   : did the headline survive day-wide elimination? (bool)
  - Rank        : final rank among survivors (lower = better; absent if culled)
  - CrossDayRank: rank among the cross-day elite (absent unless it made the cut)

Headlines are grouped by GenerateModel (the A/B variable). Because Haiku and
Sonnet headlines compete in the SAME day-wide tournament, survival rate / rank
are a direct head-to-head signal.

CAVEAT: the elimination judge is Sonnet 4.5 (Tournament MODEL_ELIMINATION), so a
same-family style preference toward the Sonnet 4.6 arm is a possible confound.

Usage (from backend/ with venv active):
    python3 Scratch/analyze_ab_judge.py --days 30
"""

import argparse
import datetime
import math
import statistics
from collections import defaultdict
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Key

HAIKU = "claude-haiku-4-5-20251001"
SONNET = "claude-sonnet-4-6"
AB_MODELS = (HAIKU, SONNET)
SHORT = {HAIKU: "Haiku 4.5", SONNET: "Sonnet 4.6"}


def all_query(tbl, **kw):
    items, r = [], tbl.query(**kw)
    items.extend(r.get("Items", []))
    while "LastEvaluatedKey" in r:
        r = tbl.query(ExclusiveStartKey=r["LastEvaluatedKey"], **kw)
        items.extend(r.get("Items", []))
    return items


def normal_cdf(z):
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def two_prop_test(s1, n1, s2, n2):
    """Two-proportion z-test. Returns (p1, p2, z, two-sided p-value)."""
    if n1 == 0 or n2 == 0:
        return None
    p1, p2 = s1 / n1, s2 / n2
    p = (s1 + s2) / (n1 + n2)
    se = math.sqrt(p * (1 - p) * (1 / n1 + 1 / n2))
    if se == 0:
        return p1, p2, 0.0, 1.0
    z = (p1 - p2) / se
    pval = 2 * (1 - normal_cdf(abs(z)))
    return p1, p2, z, pval


def fetch(days_back):
    tbl = boto3.resource("dynamodb", region_name="us-east-2").Table("SubvertedHeadlines")
    today = datetime.datetime.now(ZoneInfo("America/New_York"))
    out = []
    for d in range(days_back):
        ymd = (today - datetime.timedelta(days=d)).strftime("%Y%m%d")
        day = all_query(tbl, KeyConditionExpression=Key("YearMonthDay").eq(ymd))
        out.extend(day)
        print(f"  {ymd}: {len(day)}")
    return out


def truthy(v):
    # DynamoDB bool comes back as Python bool; guard against strings just in case.
    return v is True or v == "true" or v == 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30, help="days back to analyze")
    ap.add_argument("--top", type=int, default=16, help="top-N cutoff for 'reached top tier'")
    args = ap.parse_args()

    print(f"Querying last {args.days} days of SubvertedHeadlines...")
    items = fetch(args.days)
    print(f"Loaded {len(items)} headlines total.\n")

    # Keep only A/B-tagged headlines.
    items = [h for h in items if h.get("GenerateModel") in AB_MODELS]
    print(f"{len(items)} carry an A/B GenerateModel tag.\n")

    by_model = defaultdict(list)
    for h in items:
        by_model[h["GenerateModel"]].append(h)

    # ---- 1. Survival rate (the headline cleared day-wide elimination) ----
    print("=== SURVIVAL RATE (cleared day-wide elimination) ===")
    surv = {}
    for m in AB_MODELS:
        hs = by_model[m]
        s = sum(1 for h in hs if truthy(h.get("Survived")))
        surv[m] = (s, len(hs))
        rate = 100 * s / len(hs) if hs else 0
        print(f"  {SHORT[m]:<12} survived {s:>5}/{len(hs):<5}  ({rate:5.2f}%)")
    res = two_prop_test(surv[HAIKU][0], surv[HAIKU][1], surv[SONNET][0], surv[SONNET][1])
    if res:
        p1, p2, z, pval = res
        lead = "Sonnet" if p2 > p1 else "Haiku"
        print(f"  -> {lead} leads by {abs(p1 - p2) * 100:.2f} pts  (z={z:+.2f}, p={pval:.4g}"
              f"{'  SIGNIFICANT' if pval < 0.05 else '  n.s.'})")

    # ---- 2. Rank quality among survivors ----
    print(f"\n=== RANK AMONG SURVIVORS (lower = better) ===")
    for m in AB_MODELS:
        ranks = [int(h["Rank"]) for h in by_model[m] if h.get("Rank") is not None]
        if not ranks:
            print(f"  {SHORT[m]:<12} no ranked survivors")
            continue
        topn = sum(1 for r in ranks if r <= args.top)
        print(f"  {SHORT[m]:<12} n={len(ranks):<5} mean={statistics.mean(ranks):6.1f} "
              f"median={statistics.median(ranks):6.1f}  top-{args.top}={topn} "
              f"({100*topn/len(ranks):.1f}% of its survivors)")

    # Share of all top-N slots captured by each model.
    print(f"\n=== WHO FILLS THE TOP-{args.top} SLOTS? (composition, not rate) ===")
    topfill = defaultdict(int)
    for m in AB_MODELS:
        topfill[m] = sum(1 for h in by_model[m]
                         if h.get("Rank") is not None and int(h["Rank"]) <= args.top)
    tot = sum(topfill.values())
    for m in AB_MODELS:
        print(f"  {SHORT[m]:<12} {topfill[m]:>5} slots  ({100*topfill[m]/tot:.1f}%)" if tot else f"  {SHORT[m]}: 0")

    # ---- 3. Cross-day elite ----
    print(f"\n=== CROSS-DAY ELITE (made CrossDayRank) ===")
    cd = {}
    for m in AB_MODELS:
        hs = by_model[m]
        elite = [int(h["CrossDayRank"]) for h in hs if h.get("CrossDayRank") is not None]
        cd[m] = (len(elite), len(hs))
        if elite:
            print(f"  {SHORT[m]:<12} {len(elite):>4}/{len(hs):<5} reached cross-day "
                  f"({100*len(elite)/len(hs):.3f}%)  mean cross-day rank={statistics.mean(elite):.1f}")
        else:
            print(f"  {SHORT[m]:<12} 0 reached cross-day")
    res = two_prop_test(cd[HAIKU][0], cd[HAIKU][1], cd[SONNET][0], cd[SONNET][1])
    if res:
        p1, p2, z, pval = res
        lead = "Sonnet" if p2 > p1 else "Haiku"
        print(f"  -> {lead} leads (z={z:+.2f}, p={pval:.4g}"
              f"{'  SIGNIFICANT' if pval < 0.05 else '  n.s.'})")

    # ---- 4. Within-story head-to-head (controls for story difficulty) ----
    # For each story with BOTH a Haiku and a Sonnet headline, compare best ranks.
    # Survivor beats non-survivor; if both survive, lower rank wins; both culled = tie.
    print(f"\n=== WITHIN-STORY HEAD-TO-HEAD (controls for story difficulty) ===")
    by_story = defaultdict(lambda: defaultdict(list))
    for h in items:
        sid = h.get("StoryId")
        if sid:
            by_story[sid][h["GenerateModel"]].append(h)

    def best(hs):
        # (survived?, rank-or-inf) — smaller rank is better; survivors beat culls.
        ranked = [int(h["Rank"]) for h in hs if h.get("Rank") is not None]
        survived = any(truthy(h.get("Survived")) for h in hs)
        return (survived, min(ranked) if ranked else math.inf)

    haiku_win = sonnet_win = tie = 0
    contested = 0
    for sid, mm in by_story.items():
        if HAIKU in mm and SONNET in mm:
            contested += 1
            hsurv, hrank = best(mm[HAIKU])
            ssurv, srank = best(mm[SONNET])
            # Compare: survival first, then rank.
            if (hsurv, -hrank if hrank != math.inf else -math.inf) == \
               (ssurv, -srank if srank != math.inf else -math.inf):
                tie += 1
            elif (hsurv, -(hrank)) > (ssurv, -(srank)):
                haiku_win += 1
            else:
                sonnet_win += 1
    print(f"  stories with both models: {contested}")
    if contested:
        print(f"    Haiku  better: {haiku_win:>4} ({100*haiku_win/contested:.1f}%)")
        print(f"    Sonnet better: {sonnet_win:>4} ({100*sonnet_win/contested:.1f}%)")
        print(f"    tie / both culled: {tie:>4} ({100*tie/contested:.1f}%)")
        decided = haiku_win + sonnet_win
        if decided:
            res = two_prop_test(sonnet_win, decided, haiku_win, decided)
            # crude: among decided stories, is split != 50/50? use binomial-ish z
            p = sonnet_win / decided
            se = math.sqrt(0.25 / decided)
            z = (p - 0.5) / se if se else 0
            pval = 2 * (1 - normal_cdf(abs(z)))
            lead = "Sonnet" if sonnet_win > haiku_win else "Haiku"
            print(f"    -> among {decided} decided, {lead} wins {max(haiku_win,sonnet_win)/decided*100:.1f}% "
                  f"(z={z:+.2f}, p={pval:.4g}{'  SIGNIFICANT' if pval < 0.05 else '  n.s.'})")


if __name__ == "__main__":
    main()
