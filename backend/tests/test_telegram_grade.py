import importlib.util
import pathlib
import sys
import types
from unittest import mock

BACKEND = pathlib.Path(__file__).resolve().parents[1]
CHANNEL_CHAT = {"id": -1001, "username": "twothousanddotnews", "type": "channel"}


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


def _load_grade_module():
    commands = _load("commands_module", "TelegramScoop/commands.py")
    return _load("grade_module", "TelegramReaction/grade.py", {
        "boto3": _stub_module("boto3", resource=lambda *args, **kwargs: types.SimpleNamespace(
            Table=lambda name: None)),
        "lib.curation": _stub_module("lib.curation", TABLE_NAME="SubvertedHeadlines",
                                     apply_grade=None, claim_update=None, gen_rationale=None,
                                     rebuild_exemplar_cache=None),
        "lib.sent_headlines": _stub_module("lib.sent_headlines", find_replied_to=None,
                                           record_rationale_reply=None),
        "lib.telegram": _stub_module("lib.telegram", TelegramError=RuntimeError,
                                     send_message=None),
        "TelegramScoop": _stub_module("TelegramScoop", commands=commands),
    })


def _reply_update(text, update_id=500):
    return {"update": {"update_id": update_id, "channel_post": {
        "message_id": 42, "chat": CHANNEL_CHAT, "text": text,
        "reply_to_message": {"message_id": 41},
    }}}


class _Harness:
    def __init__(self, headline):
        self.grade = _load_grade_module()
        self.grade.find_replied_to = mock.Mock(
            return_value={"HeadlineId": "ccc10cac", "YearMonthDay": "20261008"})
        self.grade._load_headline = mock.Mock(return_value=headline)
        self.grade.apply_grade = mock.Mock(return_value=True)
        self.grade.claim_update = mock.Mock(return_value=True)
        self.grade.gen_rationale = mock.Mock(return_value="It rhymes.")
        self.grade.rebuild_exemplar_cache = mock.Mock(return_value=1)
        self.grade.record_rationale_reply = mock.Mock()
        self.grade.send_message = mock.Mock(return_value={"message_id": 43})


def test_a_reaction_grades_nothing():
    harness = _Harness({})
    reaction = {"update": {"update_id": 500, "message_reaction_count": {
        "chat": CHANNEL_CHAT, "message_id": 41,
        "reactions": [{"type": {"type": "emoji", "emoji": "🔥"}, "total_count": 900}],
    }}}

    harness.grade.handler(reaction, None)

    harness.grade.apply_grade.assert_not_called()


def test_an_emoji_reply_grades_the_headline_it_replies_to():
    harness = _Harness({"Headline": "H", "OriginalHeadline": "O"})

    harness.grade.handler(_reply_update("👎"), None)

    harness.grade.find_replied_to.assert_called_once_with(mock.ANY, -1001, 41)
    args, kwargs = harness.grade.apply_grade.call_args
    assert args[1:] == ("20261008", "ccc10cac", "bad")
    assert kwargs["telegram_update_id"] == 500
    harness.grade.send_message.assert_called_once_with(
        -1001, "👎 <b>Bad</b>", reply_to_message_id=42)


def test_an_outstanding_grade_writes_a_rationale_and_rebuilds_the_exemplars():
    harness = _Harness({"Headline": "H", "OriginalHeadline": "O"})

    harness.grade.handler(_reply_update("🔥"), None)

    assert harness.grade.apply_grade.call_args.kwargs["rationale"] == "It rhymes."
    harness.grade.record_rationale_reply.assert_called_once()
    harness.grade.rebuild_exemplar_cache.assert_called_once()


def test_regrading_with_the_same_grade_leaves_the_grade_alone():
    harness = _Harness({"Grade": "outstanding", "Rationale": "Set at the CLI."})

    harness.grade.handler(_reply_update("🔥"), None)

    harness.grade.apply_grade.assert_not_called()
    harness.grade.gen_rationale.assert_not_called()


def test_regrading_with_the_same_grade_still_supersedes_older_updates():
    harness = _Harness({"Grade": "solid"})

    harness.grade.handler(_reply_update("👍", update_id=501), None)

    harness.grade.claim_update.assert_called_once_with(mock.ANY, "20261008", "ccc10cac", 501)


def test_a_stale_update_writes_nothing():
    harness = _Harness({"Grade": "solid", "TelegramUpdateId": 501})

    harness.grade.handler(_reply_update("👎", update_id=500), None)

    harness.grade.apply_grade.assert_not_called()


def test_a_note_reply_is_not_a_grade():
    harness = _Harness({})

    harness.grade.handler(_reply_update("mention the Seuss bit"), None)

    harness.grade.apply_grade.assert_not_called()
