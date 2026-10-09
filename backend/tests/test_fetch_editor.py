import importlib.util
import pathlib
import sys
import types
from unittest import mock

BACKEND = pathlib.Path(__file__).resolve().parents[1]


def _stub_module(name, **attributes):
    module = types.ModuleType(name)
    module.__dict__.update(attributes)
    return module


def _load(relative_path, module_name, stubs):
    """Load a backend file with the given modules standing in for its
    imports, without leaving the stubs behind for other tests."""
    spec = importlib.util.spec_from_file_location(module_name, BACKEND / relative_path)
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, stubs):
        spec.loader.exec_module(module)
    return module


editor = _load("Fetch/editor.py", "fetch_editor_module", {
    "anthropic": _stub_module("anthropic"),
    "lib.ssm_secrets": _stub_module("lib.ssm_secrets", get_secret=lambda name: "secret"),
    "lib.anthropic_batches": _stub_module("lib.anthropic_batches", extract_text=lambda message: message),
    "lib.llm_json": _load("lib/llm_json.py", "llm_json_module", {}),
})


class _FakeRepo:
    def __init__(self, existing_titles=()):
        self.existing_titles = set(existing_titles)
        self.saved = []
        self.mashups = []

    def is_new(self, story):
        return story.get("image_url") is not None and story["title"] not in self.existing_titles

    def save_story(self, story, fetch_category, extra_attributes=None):
        self.saved.append((story["title"], fetch_category, extra_attributes))
        return story

    def save_mashup(self, stories, editor_note):
        self.mashups.append(([story["title"] for story in stories], editor_note))
        return stories


fetch = _load("Fetch/fetch.py", "fetch_module", {
    "Fetch.editor": editor,
    "lib.newsdata_client": _stub_module("lib.newsdata_client", NewsdataClient=lambda: None),
    "lib.rss_client": _stub_module("lib.rss_client", RssClient=lambda: None),
    "lib.stories_repository": _stub_module("lib.stories_repository", StoriesRepository=lambda: None),
})


def _story(title, description="", image_url="https://img.example/x.jpg", source_id="nytimes.com"):
    return {"title": title, "description": description, "image_url": image_url, "source_id": source_id}


def _candidates(*titles, label="nyt_homepage"):
    return [editor.Candidate(story=_story(t), label=label) for t in titles]


def _titles(candidates):
    return [c.story["title"] for c in candidates]


def _source_titles(picks):
    return [[c.story["title"] for c in pick.sources] for pick in picks]


def test_prompt_states_the_count_before_and_after_the_listing():
    prompt = editor.build_prompt(_candidates("A", "B"), 21, 5)

    assert prompt.startswith("Pick the 21 stories to assign, up to 5 of them mashups.\n\n[1] A")
    assert prompt.endswith("\n\nPick the 21 stories to assign, up to 5 of them mashups.")


def test_prompt_numbers_candidates_from_one_and_flattens_descriptions():
    candidates = [
        editor.Candidate(story=_story("Mayor Declares War on Geese", "The mayor\n\nsaid  so."), label="nyt_homepage"),
        editor.Candidate(story=_story("Bengals Win", source_id="espn.com"), label="espn_top"),
    ]

    prompt = editor.build_prompt(candidates, 1, 0)

    assert "[1] Mayor Declares War on Geese\n(nytimes.com) The mayor said so." in prompt
    assert "[2] Bengals Win\n(espn.com)" in prompt


def test_prompt_truncates_long_descriptions():
    candidate = editor.Candidate(story=_story("Title", "word " * 200), label="nyt_world")

    prompt = editor.build_prompt([candidate], 1, 0)

    description_line = next(line for line in prompt.splitlines() if line.startswith("(nytimes.com)"))
    assert description_line.endswith("…")
    assert len(description_line) < editor.DESCRIPTION_CHARS + 20


def test_picks_map_ids_to_candidates_in_the_editors_order_with_notes():
    candidates = _candidates("A", "B", "C")

    picks = editor.parse_picks(
        '[{"ids": [3], "why": "absurd"}, {"ids": [1, 2], "why": " collide "}]', candidates, 5, 5,
    )

    assert _source_titles(picks) == [["C"], ["A", "B"]]
    assert [p.note for p in picks] == ["absurd", "collide"]
    assert [p.is_mashup for p in picks] == [False, True]


def test_picks_skip_unknown_repeated_and_malformed_ids():
    candidates = _candidates("A", "B", "C", "D", "E")

    picks = editor.parse_picks(
        '[{"ids": [0]}, {"ids": [6]}, {"ids": ["1"]}, {"id": 1}, "junk", {"ids": []},'
        ' {"ids": [1, 1]}, {"ids": [1, 2, 3, 4]}, {"ids": [2], "why": "x"}, {"ids": [2], "why": "again"},'
        ' {"ids": [3, 4]}, {"ids": [4, 3]}]',
        candidates, 21, 5,
    )

    assert _source_titles(picks) == [["B"], ["C", "D"]]


def test_a_story_can_run_alone_and_in_a_mashup():
    picks = editor.parse_picks('[{"ids": [1]}, {"ids": [1, 2]}]', _candidates("A", "B"), 21, 5)

    assert _source_titles(picks) == [["A"], ["A", "B"]]


def test_picks_skip_mashups_past_the_cap():
    picks = editor.parse_picks(
        '[{"ids": [1, 2]}, {"ids": [3, 4]}, {"ids": [5]}]', _candidates("A", "B", "C", "D", "E"), 21, 1,
    )

    assert _source_titles(picks) == [["A", "B"], ["E"]]


def test_picks_stop_at_count():
    picks = editor.parse_picks('[{"ids": [1]}, {"ids": [2]}, {"ids": [3]}]', _candidates("A", "B", "C"), 2, 5)

    assert _source_titles(picks) == [["A"], ["B"]]


def test_gather_caps_each_source_at_its_pool_and_drops_repeats_and_saved_stories():
    feeds = {
        "nyt_homepage": [_story("Shared"), _story("Already Saved"), _story("Home 1"), _story("Home 2"), _story("Home 3")],
        "nyt_politics": [_story("Shared"), _story("No Image", image_url=None), _story("Politics 1")],
    }
    plans = [
        {"label": "nyt_homepage", "pool": 3},
        {"label": "nyt_politics", "pool": 3},
    ]

    with mock.patch.object(fetch, "_repo", _FakeRepo(existing_titles={"Already Saved"})), \
            mock.patch.object(fetch, "_stories_from", lambda plan: iter(feeds[plan["label"]])):
        candidates = fetch.gather_candidates(plans)

    assert [(c.label, c.story["title"]) for c in candidates] == [
        ("nyt_homepage", "Shared"),
        ("nyt_homepage", "Home 1"),
        ("nyt_homepage", "Home 2"),
        ("nyt_politics", "Politics 1"),
    ]


def test_gather_keeps_going_when_one_source_fails():
    def stories_from(plan):
        if plan["label"] == "broken":
            raise OSError("feed down")
        return iter([_story("Works")])

    with mock.patch.object(fetch, "_repo", _FakeRepo()), \
            mock.patch.object(fetch, "_stories_from", stories_from):
        candidates = fetch.gather_candidates([{"label": "broken", "pool": 2}, {"label": "ok", "pool": 2}])

    assert _titles(candidates) == ["Works"]


def test_save_order_falls_back_to_each_sources_quota_when_the_editor_fails():
    candidates = _candidates("H1", "H2", "H3", label="nyt_homepage") + _candidates("W1", "W2", label="nyt_world")
    plans = [{"label": "nyt_homepage", "n": 2}, {"label": "nyt_world", "n": 1}]

    def broken_editor(candidates, count, max_mashups):
        raise RuntimeError("overloaded")

    with mock.patch.object(fetch, "pick_stories", broken_editor), \
            mock.patch.object(fetch, "EDITOR_PLAN", plans):
        order = fetch.save_order(candidates)

    assert _source_titles(order) == [["H1"], ["H2"], ["W1"], ["H3"], ["W2"]]


def test_save_order_backfills_after_the_editors_picks_without_repeating_their_stories():
    candidates = _candidates("H1", "H2", "H3", "H4")

    def editor_picks(candidates, count, max_mashups):
        return [editor.Pick(sources=(candidates[2],)), editor.Pick(sources=(candidates[3], candidates[0]))]

    with mock.patch.object(fetch, "pick_stories", editor_picks), \
            mock.patch.object(fetch, "EDITOR_PLAN", [{"label": "nyt_homepage", "n": 1}]):
        order = fetch.save_order(candidates)

    assert _source_titles(order) == [["H3"], ["H4", "H1"], ["H2"]]


def test_fetch_saves_pinned_stories_then_the_editors_picks_with_their_notes():
    repo = _FakeRepo()
    pinned = [{"label": "bengals_hobson", "n": 1}]
    plans = [{"label": "nyt_homepage", "n": 1, "pool": 5}]
    feeds = {"bengals_hobson": [_story("Hobson")], "nyt_homepage": [_story("Dull"), _story("Ripe")]}

    def editor_picks(candidates, count, max_mashups):
        return [editor.Pick(sources=(candidates[1],), note="it writes itself")]

    with mock.patch.object(fetch, "_repo", repo), \
            mock.patch.object(fetch, "_stories_from", lambda plan: iter(feeds[plan["label"]])), \
            mock.patch.object(fetch, "pick_stories", editor_picks), \
            mock.patch.object(fetch, "PINNED_PLAN", pinned), \
            mock.patch.object(fetch, "EDITOR_PLAN", plans), \
            mock.patch.object(fetch, "EDITOR_PICK_COUNT", 1):
        fetch.fetch({}, None)

    assert repo.saved == [
        ("Hobson", "bengals_hobson", None),
        ("Ripe", "nyt_homepage", {"EditorNote": "it writes itself"}),
    ]


def test_fetch_backfills_slots_the_editor_left_empty_or_a_save_turned_down():
    class RejectingRepo(_FakeRepo):
        def save_story(self, story, fetch_category, extra_attributes=None):
            if story["title"] == "Raced":
                return None
            return super().save_story(story, fetch_category, extra_attributes)

    repo = RejectingRepo()
    plans = [{"label": "nyt_homepage", "n": 1, "pool": 5}]
    feeds = {"nyt_homepage": [_story("Raced"), _story("Backup 1"), _story("Backup 2"), _story("Backup 3")]}

    with mock.patch.object(fetch, "_repo", repo), \
            mock.patch.object(fetch, "_stories_from", lambda plan: iter(feeds[plan["label"]])), \
            mock.patch.object(fetch, "pick_stories", lambda candidates, count, max_mashups: [editor.Pick(sources=(candidates[0],))]), \
            mock.patch.object(fetch, "PINNED_PLAN", []), \
            mock.patch.object(fetch, "EDITOR_PLAN", plans), \
            mock.patch.object(fetch, "EDITOR_PICK_COUNT", 2):
        fetch.fetch({}, None)

    assert [title for title, _, _ in repo.saved] == ["Backup 1", "Backup 2"]


def test_fetch_files_a_mashup_as_one_story_that_fills_one_slot():
    repo = _FakeRepo()
    plans = [{"label": "nyt_homepage", "n": 0, "pool": 5}]
    feeds = {"nyt_homepage": [_story("Yacht"), _story("Union"), _story("Spare")]}

    def editor_picks(candidates, count, max_mashups):
        return [editor.Pick(sources=(candidates[0], candidates[1]), note="the collision")]

    with mock.patch.object(fetch, "_repo", repo), \
            mock.patch.object(fetch, "_stories_from", lambda plan: iter(feeds[plan["label"]])), \
            mock.patch.object(fetch, "pick_stories", editor_picks), \
            mock.patch.object(fetch, "PINNED_PLAN", []), \
            mock.patch.object(fetch, "EDITOR_PLAN", plans), \
            mock.patch.object(fetch, "EDITOR_PICK_COUNT", 2):
        result = fetch.fetch({}, None)

    assert repo.mashups == [(["Yacht", "Union"], "the collision")]
    assert [title for title, _, _ in repo.saved] == ["Spare"]
    assert "'mashup': 1" in result
