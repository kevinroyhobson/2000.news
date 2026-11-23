import importlib.util
import pathlib
import sys
import types
from unittest import mock

BACKEND = pathlib.Path(__file__).resolve().parents[1]
CHANNEL_CHAT = {"id": -1001, "username": "twothousanddotnews", "type": "channel"}
PERMALINK = "https://www.2000.news/20261122/5c31ed2b"
CARD = "Trump Goes All In On One Card, Insists It Is Very Good Card, Maybe Best Card"
GRAPHICS_CARD = "Trump Goes All In On One Graphics Card, Insists It Is Very Good Card, Maybe Best Card"


def _stub_module(name, **attributes):
    module = types.ModuleType(name)
    module.__dict__.update(attributes)
    return module


def _load(name, relative_path, stubs=None):
    spec = importlib.util.spec_from_file_location(name, BACKEND / relative_path)
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, stubs or {}), \
            mock.patch.dict("os.environ", {"TELEGRAM_CHAT_ID": "@twothousanddotnews"}):
        spec.loader.exec_module(module)
    return module


def _load_alter_module():
    commands = _load("commands_module", "TelegramScoop/commands.py")
    return _load("alter_module", "TelegramAlter/alter.py", {
        "boto3": _stub_module("boto3", resource=lambda *args, **kwargs: types.SimpleNamespace(
            Table=lambda name: None)),
        "lib.curation": _stub_module("lib.curation", TABLE_NAME="SubvertedHeadlines",
                                     alter_headline=None, rebuild_exemplar_cache=None,
                                     set_headline=None),
        "lib.telegram": _stub_module("lib.telegram", TelegramError=RuntimeError, send_message=None),
        "TelegramScoop": _stub_module("TelegramScoop", commands=commands),
    })


def _alter_update(text):
    return {"update": {"update_id": 500, "channel_post": {
        "message_id": 42, "chat": CHANNEL_CHAT, "text": text,
    }}}


class _Harness:
    def __init__(self, headline, altered=GRAPHICS_CARD):
        self.alter = _load_alter_module()
        self.alter._headlines_table = mock.Mock()
        self.alter._headlines_table.get_item.return_value = {"Item": headline} if headline else {}
        self.alter.alter_headline = mock.Mock(return_value=altered)
        self.alter.set_headline = mock.Mock(return_value=True)
        self.alter.rebuild_exemplar_cache = mock.Mock(return_value=1)
        self.alter.send_message = mock.Mock()

    def reply(self):
        return self.alter.send_message.call_args.args[1]


def test_alter_rewrites_the_headline_and_replies_with_its_permalink():
    harness = _Harness({"Headline": CARD, "OriginalHeadline": "Trump bets on Nvidia", "Grade": "solid"})
    harness.alter.handler(_alter_update(f"/alter {PERMALINK} say graphics card the first time"), None)

    harness.alter.alter_headline.assert_called_once_with(
        CARD, "Trump bets on Nvidia", "say graphics card the first time", on_refusal=mock.ANY)
    harness.alter.set_headline.assert_called_once_with(
        mock.ANY, "20261122", "5c31ed2b", CARD, GRAPHICS_CARD, "say graphics card the first time")
    assert harness.reply() == f"<b>{GRAPHICS_CARD}</b>\n\nWas: {CARD}\n\n{PERMALINK}"
    harness.alter.rebuild_exemplar_cache.assert_not_called()


def test_altering_an_outstanding_headline_rebuilds_the_exemplars():
    harness = _Harness({"Headline": CARD, "Grade": "outstanding"})
    harness.alter.handler(_alter_update(f"/alter {PERMALINK} graphics card"), None)
    harness.alter.rebuild_exemplar_cache.assert_called_once()


def test_a_failed_reply_still_rebuilds_the_exemplars():
    harness = _Harness({"Headline": CARD, "Grade": "outstanding"})
    harness.alter.send_message.side_effect = RuntimeError("sendMessage HTTP 502")
    harness.alter.handler(_alter_update(f"/alter {PERMALINK} graphics card"), None)
    harness.alter.rebuild_exemplar_cache.assert_called_once()


def test_an_unchanged_headline_writes_nothing():
    harness = _Harness({"Headline": CARD}, altered=CARD)
    harness.alter.handler(_alter_update(f"/alter {PERMALINK} leave it"), None)
    harness.alter.set_headline.assert_not_called()
    assert harness.reply().startswith("Left it as is")


def test_a_headline_changed_mid_alter_is_not_overwritten():
    harness = _Harness({"Headline": CARD, "Grade": "outstanding"})
    harness.alter.set_headline.return_value = False
    harness.alter.handler(_alter_update(f"/alter {PERMALINK} graphics card"), None)
    assert "changed while I was working on it" in harness.reply()
    harness.alter.rebuild_exemplar_cache.assert_not_called()


def test_a_missing_headline_says_so():
    harness = _Harness(None)
    harness.alter.handler(_alter_update(f"/alter {PERMALINK} graphics card"), None)
    harness.alter.alter_headline.assert_not_called()
    assert harness.reply() == "No headline at that link."


def test_alter_without_an_instruction_replies_with_usage():
    harness = _Harness({"Headline": CARD})
    harness.alter.handler(_alter_update(f"/alter {PERMALINK}"), None)
    harness.alter.alter_headline.assert_not_called()
    assert harness.reply() == harness.alter.USAGE


def test_headlines_are_escaped_in_the_reply():
    reply = _load_alter_module().altered_reply("A & B", "A < B", PERMALINK)
    assert reply == f"<b>A &lt; B</b>\n\nWas: A &amp; B\n\n{PERMALINK}"
