import datetime
import importlib.util
import json
import pathlib
import sys
import types
from unittest import mock

import pytest

BACKEND = pathlib.Path(__file__).resolve().parents[1]
TODAY = datetime.date(2026, 11, 1)


def _load(relative_path, module_name, stubs):
    spec = importlib.util.spec_from_file_location(module_name, BACKEND / relative_path)
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, stubs):
        spec.loader.exec_module(module)
    return module


def _extract_text(message):
    return next(block.text for block in message.content if block.type == "text")


llm_json = _load("lib/llm_json.py", "llm_json_module", {})
editor = _load("TelegramScoop/editor.py", "scoop_editor_module", {
    "lib.anthropic_batches": types.SimpleNamespace(extract_text=_extract_text),
    "lib.llm_json": llm_json,
})


def _result(title, image="https://img.example/1.jpg", source="cnn"):
    return {"title": title, "description": f"About {title}.", "link": f"https://news.example/{len(title)}",
            "image_url": image, "source_id": source}


def _filed(title):
    return {"Title": title, "Description": "Filed earlier.", "Url": "https://nyt.example/1",
            "Source": "nytimes.com", "YearMonthDay": "20261031", "StoryId": "abcde"}


def _text(text):
    return types.SimpleNamespace(type="text", text=text)


def _search(query, title_only=True, tool_id="tool-1"):
    return types.SimpleNamespace(type="tool_use", id=tool_id,
                                 input={"query": query, "title_only": title_only})


def _response(*content, stop_reason="end_turn"):
    return types.SimpleNamespace(content=list(content), stop_reason=stop_reason)


class _FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []
        self.beta = types.SimpleNamespace(messages=types.SimpleNamespace(create=self._create))

    def _create(self, **request):
        self.requests.append({**request, "messages": list(request["messages"])})
        return self.responses.pop(0)


class _FakeNewsdata:
    def __init__(self, results):
        self.results = results
        self.queries = []

    def fetch_by_query(self, query, title_only=False):
        self.queries.append((query, title_only))
        return {"results": self.results}


def _assign(client, newsdata, unwritten=(), unavailable=None, max_picks=3):
    return editor.assign("haley stevens endorsing mike rogers", client, newsdata,
                         list(unwritten), unavailable or {}, max_picks, TODAY)


def test_desk_numbers_filed_stories_first_and_keeps_a_title_its_first_number():
    desk = editor.Desk([_filed("Why Haley Stevens Can’t Stop Talking")], {})
    assert desk.add_result(_result("Stevens Endorses Rogers")) == 2
    assert desk.add_result(_result("Why Haley Stevens Can’t Stop Talking")) == 1
    assert desk.add_result(_result("Stevens Endorses Rogers")) == 2


def test_desk_marks_stories_that_cant_run():
    desk = editor.Desk([], {"Trump makes his last red-state stops": "the paper already ran it"})
    ran = desk.add_result(_result("Trump makes his last red-state stops"))
    no_photo = desk.add_result(_result("Stevens Endorses Rogers", image=None))
    assert desk.describe(ran).endswith("Can't run: the paper already ran it.")
    assert desk.describe(no_photo).endswith("Can't run: it has no photo.")


def test_parse_assignment_keeps_only_runnable_picks_up_to_the_cap():
    desk = editor.Desk([], {"Already Ran": "the paper already ran it"})
    for title in ("One", "Already Ran", "Two", "Three"):
        desk.add_result(_result(title))
    answer = json.dumps({"picks": [{"id": 1, "why": "first"}, {"id": 2, "why": "ran"}, {"id": 1, "why": "again"},
                                   {"id": 99, "why": "nothing"}, {"id": 3, "why": "second"}, {"id": 4, "why": "over"}],
                         "close_calls": [3, 4]})
    assignment = editor.parse_assignment(f"```json\n{answer}\n```", desk, max_picks=2)
    assert [(pick.candidate.title, pick.note) for pick in assignment.picks] == [("One", "first"), ("Two", "second")]
    assert [candidate.title for candidate in assignment.close_calls] == ["Three"]


def test_parse_assignment_of_an_unreadable_answer_picks_nothing():
    assignment = editor.parse_assignment("I couldn't find anything.", editor.Desk([], {}), max_picks=3)
    assert assignment.picks == [] and assignment.close_calls == []


def test_assign_searches_then_picks_from_the_results():
    newsdata = _FakeNewsdata([_result("Stevens Endorses Rogers in Michigan Senate Race")])
    client = _FakeClient([
        _response(_search('"Haley Stevens" AND Rogers'), stop_reason="tool_use"),
        _response(_text('{"picks": [{"id": 1, "why": "A Democrat endorsing the Republican."}], "close_calls": []}')),
    ])
    assignment = _assign(client, newsdata)

    assert newsdata.queries == [('"Haley Stevens" AND Rogers', True)]
    assert [(p.candidate.title, p.note) for p in assignment.picks] == [
        ("Stevens Endorses Rogers in Michigan Senate Race", "A Democrat endorsing the Republican.")]
    tool_result = client.requests[1]["messages"][-1]["content"][0]
    assert tool_result["tool_use_id"] == "tool-1"
    assert tool_result["content"].startswith("[1] Stevens Endorses Rogers in Michigan Senate Race")


def test_assign_can_pick_a_filed_story_without_searching():
    client = _FakeClient([_response(_text('{"picks": [{"id": 1, "why": "Her own interview."}]}'))])
    assignment = _assign(client, _FakeNewsdata([]), unwritten=[_filed("Why Haley Stevens Can’t Stop Talking")])
    assert assignment.picks[0].candidate.filed["StoryId"] == "abcde"
    assert "[1] Why Haley Stevens Can’t Stop Talking" in client.requests[0]["messages"][0]["content"]


def test_searches_past_the_budget_are_turned_away():
    newsdata = _FakeNewsdata([])
    searches = [_search(f"query {i}", tool_id=f"tool-{i}") for i in range(editor.MAX_SEARCHES + 1)]
    client = _FakeClient([
        _response(*searches, stop_reason="tool_use"),
        _response(_text('{"picks": [], "close_calls": []}')),
    ])
    _assign(client, newsdata)

    assert len(newsdata.queries) == editor.MAX_SEARCHES
    turned_away = client.requests[1]["messages"][-1]["content"][-1]
    assert turned_away["is_error"] is True


def test_a_refusal_is_reported_to_the_channel():
    client = _FakeClient([_response(stop_reason="refusal")])
    with pytest.raises(editor.EditorError):
        _assign(client, _FakeNewsdata([]))


def test_an_editor_that_never_stops_searching_is_cut_off():
    client = _FakeClient([_response(_search("again", tool_id=f"t{i}"), stop_reason="tool_use")
                          for i in range(editor.MAX_TURNS)])
    with pytest.raises(editor.EditorError):
        _assign(client, _FakeNewsdata([]))
