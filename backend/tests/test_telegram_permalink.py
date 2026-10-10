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


def _load_telegram_module():
    spec = importlib.util.spec_from_file_location("telegram_module", BACKEND / "lib" / "telegram.py")
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, {"lib.ssm_secrets": _stub_module("lib.ssm_secrets", get_secret=None)}):
        spec.loader.exec_module(module)
    return module


def _load_permalink_module():
    spec = importlib.util.spec_from_file_location(
        "permalink_module", BACKEND / "TelegramReaction" / "permalink.py")
    module = importlib.util.module_from_spec(spec)
    stubs = {
        "boto3": _stub_module("boto3", resource=lambda *args, **kwargs: types.SimpleNamespace(
            Table=lambda name: None)),
        "lib.curation": _stub_module("lib.curation", TABLE_NAME="SubvertedHeadlines"),
        "lib.sent_headlines": _stub_module("lib.sent_headlines", record_post=None),
        "lib.telegram": _load_telegram_module(),
        "TelegramScoop": _stub_module("TelegramScoop", commands=None),
    }
    with mock.patch.dict(sys.modules, stubs), mock.patch.dict("os.environ", {"TELEGRAM_CHAT_ID": "@x"}):
        spec.loader.exec_module(module)
    return module


permalink = _load_permalink_module()


def test_headline_links_the_real_one_it_riffs_on():
    text = permalink.format_headline(
        {"Headline": "Fed Raises Rates & Eyebrows", "OriginalHeadline": "Fed raises rates"},
        {"Title": "Fed raises rates", "Url": "https://a.example/fed", "Source": "nytimes.com"},
    )
    assert text == ('Fed Raises Rates &amp; Eyebrows\n\n'
                    '(<a href="https://a.example/fed">Fed raises rates</a>, nytimes.com)')


def test_mashup_links_every_source_on_its_own_line():
    text = permalink.format_headline(
        {"Headline": "H", "OriginalHeadline": "A / B", "SourceHeadlines": ["A", "B"]},
        {"Title": "A / B", "SourceStories": [
            {"Title": "A", "Url": "https://a.example/a", "Source": "nytimes.com"},
            {"Title": "B", "Url": "https://b.example/b", "Source": "espn.com"},
        ]},
    )
    assert text == ('H\n\n(<a href="https://a.example/a">A</a>, nytimes.com)\n'
                    '(<a href="https://b.example/b">B</a>, espn.com)')


def test_real_headlines_still_show_when_the_story_row_is_gone():
    text = permalink.format_headline(
        {"Headline": "H", "OriginalHeadline": "A / B", "SourceHeadlines": ["A", "B"]}, {},
    )
    assert text == "H\n\n(A)\n(B)"


def test_story_row_is_looked_up_by_the_headlines_day_and_original_headline():
    class Stories:
        def get_item(self, Key):
            assert Key == {"YearMonthDay": "20261009", "Title": "A / B"}
            return {"Item": {"Title": "A / B"}}

    with mock.patch.object(permalink, "_stories_table", Stories()):
        story = permalink._story_for({"YearMonthDay": "20261009", "OriginalHeadline": "A / B"})

    assert story == {"Title": "A / B"}


def test_existing_grade_is_called_out():
    text = permalink.format_headline({"Headline": "H", "OriginalHeadline": "A", "Grade": "solid"}, {})
    assert text.endswith("Currently graded <b>solid</b>.")
