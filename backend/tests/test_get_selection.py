import importlib.util
import pathlib
import sys
import types


class _DummyTable:
    def query(self, **kwargs):
        return {"Items": []}

    def scan(self, **kwargs):
        return {"Items": []}


class _DummyDynamo:
    def Table(self, name):
        return _DummyTable()


def _install_boto3_stub():
    boto3 = types.ModuleType("boto3")
    boto3.resource = lambda *args, **kwargs: _DummyDynamo()
    sys.modules.setdefault("boto3", boto3)

    conditions = types.ModuleType("boto3.dynamodb.conditions")

    class Key:
        def __init__(self, *args, **kwargs):
            pass

        def eq(self, value):
            return self

    conditions.Key = Key
    sys.modules.setdefault("boto3.dynamodb.conditions", conditions)


def _load_get_module():
    _install_boto3_stub()
    path = pathlib.Path(__file__).resolve().parents[1] / "Get" / "get.py"
    spec = importlib.util.spec_from_file_location("get_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _headline(headline_id, rank, grade=None, story_id=None):
    return {
        "HeadlineId": headline_id,
        "Headline": headline_id,
        "Rank": rank,
        "StoryId": story_id or headline_id,
        "YearMonthDay": "20260712",
        "Grade": grade,
    }


def test_seen_as_top_picks_lowest_unseen_rank():
    get = _load_get_module()
    headlines = [
        _headline("rank-21", 21),
        _headline("rank-2", 2),
        _headline("rank-1-seen", 1),
        _headline("rank-3", 3),
    ]

    selected = get.select_headlines(
        headlines,
        requested_headline_id="",
        search_query="",
        rank_field="Rank",
        seen_as_top={"rank-1-seen"},
    )

    assert selected[0]["HeadlineId"] == "rank-2"


def test_seen_as_top_uses_cross_day_rank_when_supplied():
    get = _load_get_module()
    headlines = [
        {**_headline("daily-rank-1-cross-10", 1), "CrossDayRank": 10},
        {**_headline("daily-rank-9-cross-2", 9), "CrossDayRank": 2},
    ]

    selected = get.select_headlines(
        headlines,
        requested_headline_id="",
        search_query="",
        rank_field="CrossDayRank",
        seen_as_top=set(),
    )

    assert selected[0]["HeadlineId"] == "daily-rank-9-cross-2"


def test_enriched_story_uses_selected_rank_field():
    get = _load_get_module()
    headline = {
        **_headline("daily-rank-21-cross-2", 21),
        "CrossDayRank": 2,
        "OriginalHeadline": "Original",
    }

    get.get_stories_for_day = lambda day: [{
        "StoryId": headline["StoryId"],
        "Title": "Title",
    }]

    [story] = get.enrich_with_story_details(
        [headline],
        [headline],
        rank_field="CrossDayRank",
    )

    assert story["Rank"] == 2


def test_sibling_headlines_use_selected_rank_field():
    get = _load_get_module()
    selected = {
        **_headline("daily-rank-21-cross-2", 21, story_id="story"),
        "CrossDayRank": 2,
        "OriginalHeadline": "Original",
    }
    sibling = {
        **_headline("daily-rank-1-cross-10", 1, story_id="story"),
        "CrossDayRank": 10,
    }

    get.get_stories_for_day = lambda day: [{
        "StoryId": selected["StoryId"],
        "Title": "Title",
    }]

    [story] = get.enrich_with_story_details(
        [selected],
        [selected, sibling],
        rank_field="CrossDayRank",
    )

    assert [h["HeadlineId"] for h in story["SiblingHeadlines"]] == [
        "daily-rank-21-cross-2",
        "daily-rank-1-cross-10",
    ]
    assert [h["Rank"] for h in story["SiblingHeadlines"]] == [2, 10]
