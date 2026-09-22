"""Rank the headlines one channel request produced, right now, for its reply.

The same judge prompt and models as the Tournament pipeline, run as
synchronous calls over a pool small enough for that to be cheap: groups of
about 15 feed a single final, the way the real thing works, minus the
ensemble. Nothing is written back; the scheduled tournament ranks these
headlines for real later.
"""

import random

from lib.anthropic_batches import run_synchronously
from Tournament.pipeline import (
    EFFORT_ELIMINATION,
    EFFORT_FINAL,
    MODEL_ELIMINATION,
    MODEL_FINAL,
    build_ranking_request,
    distribute_into_groups,
    get_anthropic_client,
    parse_ranking,
)

GROUP_SIZE = 15
FINAL_SIZE = 20


def rank(headlines: list) -> list:
    """headlines: SubvertedHeadlines items. Returns them best first."""
    if len(headlines) < 2:
        return list(headlines)
    pool = list(headlines)
    random.shuffle(pool)
    if len(pool) > FINAL_SIZE:
        pool = _finalists(pool)
    return _ranked(pool, model=MODEL_FINAL, effort=EFFORT_FINAL)


def _finalists(pool: list) -> list:
    groups = distribute_into_groups(pool, max(2, round(len(pool) / GROUP_SIZE)))
    advancing = FINAL_SIZE // len(groups)
    requests = [_request(f"group-{i}", group, model=MODEL_ELIMINATION, effort=EFFORT_ELIMINATION)
                for i, group in enumerate(groups)]
    results = run_synchronously(get_anthropic_client(), requests)
    finalists = []
    for i, group in enumerate(groups):
        finalists.extend(_in_judged_order(group, results.get(f"group-{i}", {}))[:advancing])
    return finalists


def _ranked(group: list, *, model: str, effort: str) -> list:
    request = _request("final", group, model=model, effort=effort)
    result = run_synchronously(get_anthropic_client(), [request])["final"]
    return _in_judged_order(group, result)


def _request(custom_id: str, group: list, *, model: str, effort: str) -> dict:
    group_data = [{"headline": h["Headline"], "original_headline": h.get("OriginalHeadline", "")}
                  for h in group]
    return build_ranking_request(custom_id, group_data, remaining=len(group),
                                 model=model, effort=effort, cross_day=False)


def _in_judged_order(group: list, result: dict) -> list:
    """An errored call leaves the group in random order, as an unparseable
    answer does, so one bad request never sinks the whole reply."""
    order, _ = parse_ranking(result.get("text", ""), len(group))
    return [group[i] for i in order]
