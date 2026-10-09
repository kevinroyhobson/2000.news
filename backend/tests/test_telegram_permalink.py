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


def _load_permalink_module():
    spec = importlib.util.spec_from_file_location(
        "permalink_module", BACKEND / "TelegramReaction" / "permalink.py")
    module = importlib.util.module_from_spec(spec)
    stubs = {
        "boto3": _stub_module("boto3", resource=lambda *args, **kwargs: types.SimpleNamespace(
            Table=lambda name: None)),
        "lib.curation": _stub_module("lib.curation", TABLE_NAME="SubvertedHeadlines"),
        "lib.sent_headlines": _stub_module("lib.sent_headlines", record_post=None),
        "lib.telegram": _stub_module("lib.telegram", send_message=None),
        "TelegramScoop": _stub_module("TelegramScoop", commands=None),
    }
    with mock.patch.dict(sys.modules, stubs), mock.patch.dict("os.environ", {"TELEGRAM_CHAT_ID": "@x"}):
        spec.loader.exec_module(module)
    return module


permalink = _load_permalink_module()


def test_headline_is_shown_with_the_real_one_it_riffs_on():
    text = permalink.format_headline({"Headline": "Fed Raises Rates & Eyebrows",
                                      "OriginalHeadline": "Fed raises rates"})
    assert text == "Fed Raises Rates &amp; Eyebrows\n\n(Fed raises rates)"


def test_mashup_names_every_source():
    text = permalink.format_headline({"Headline": "H", "OriginalHeadline": "A",
                                      "SourceHeadlines": ["A", "B"]})
    assert text == "H\n\n(A + B)"


def test_existing_grade_is_called_out():
    text = permalink.format_headline({"Headline": "H", "OriginalHeadline": "A", "Grade": "solid"})
    assert text.endswith("Currently graded <b>solid</b>.")
