import importlib.util
import pathlib
import sys
import types
from unittest import mock

BACKEND = pathlib.Path(__file__).resolve().parents[1]


def _load(relative_path, module_name, stubs):
    spec = importlib.util.spec_from_file_location(module_name, BACKEND / relative_path)
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, stubs):
        spec.loader.exec_module(module)
    return module


def _stub_module(name, **attributes):
    module = types.ModuleType(name)
    module.__dict__.update(attributes)
    return module


repository = _load("lib/stories_repository.py", "stories_repository_module", {
    "boto3": _stub_module("boto3"),
    "botocore": _stub_module("botocore"),
    "botocore.exceptions": _stub_module("botocore.exceptions", ClientError=Exception),
})

telegram_alert = _load("TelegramAlert/telegram_alert.py", "telegram_alert_module", {
    "boto3": _stub_module("boto3", resource=lambda *args, **kwargs: types.SimpleNamespace(Table=lambda name: None)),
    "lib.telegram": _stub_module("lib.telegram", TelegramError=Exception, send_message=None),
})


def _story(title, pub_date, source_id, description="", keywords=None):
    return {
        "title": title, "pubDate": pub_date, "source_id": source_id, "description": description,
        "link": f"https://{source_id}/{title}", "image_url": f"https://{source_id}/{title}.jpg",
        "keywords": keywords,
    }


def test_mashup_item_joins_its_sources_and_leads_with_the_first():
    item = repository.mashup_item([
        _story("Yacht", "2026-10-08T23:00:00+00:00", "nytimes.com", "It has a helipad.", ["Bezos"]),
        _story("Union", "2026-10-09 08:00:00", "espn.com", "The vote passed."),
        _story("Strike", "2026-10-09T09:00:00+00:00", "nytimes.com", None, ["labor"]),
    ], "the collision")

    assert item["Title"] == "Yacht / Union / Strike"
    assert item["YearMonthDay"] == "20261009"
    assert item["ImageUrl"] == "https://nytimes.com/Yacht.jpg"
    assert item["Url"] == "https://nytimes.com/Yacht"
    assert item["Source"] == "nytimes.com + espn.com"
    assert item["Keywords"] == ["Bezos", "labor"]
    assert item["Category"] is None
    assert item["FetchCategory"] == "mashup"
    assert item["EditorNote"] == "the collision"
    assert item["SourceStories"][1] == {
        "Title": "Union", "Description": "The vote passed.", "Url": "https://espn.com/Union", "Source": "espn.com",
    }
    assert item["SourceStories"][2]["Description"] == ""


def test_telegram_alert_links_every_source_of_a_mashup():
    message = telegram_alert._format_message({
        "Headline": "Billionaire Unionizes Yacht",
        "YearMonthDay": "20261009",
        "HeadlineId": "abc",
        "SourceStories": [
            {"Title": "Yacht & Co", "Url": "https://a.example/y", "Source": "nytimes.com"},
            {"Title": "Union", "Url": "", "Source": ""},
        ],
    })

    assert message.endswith(
        '\n\n(<a href="https://a.example/y">Yacht &amp; Co</a>, nytimes.com)\n(Union)'
    )


def test_telegram_alert_attributes_a_single_story_as_before():
    message = telegram_alert._format_message({
        "Headline": "Fake",
        "YearMonthDay": "20261009",
        "HeadlineId": "abc",
        "OriginalHeadline": "Real",
        "Url": "https://a.example/r",
        "Source": "espn.com",
    })

    assert message == (
        'Fake\n\nhttps://www.2000.news/20261009/abc\n\n(<a href="https://a.example/r">Real</a>, espn.com)'
    )
