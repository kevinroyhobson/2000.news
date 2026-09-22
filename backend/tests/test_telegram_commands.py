import importlib.util
import pathlib


def _load_commands_module():
    path = pathlib.Path(__file__).resolve().parents[1] / "TelegramScoop" / "commands.py"
    spec = importlib.util.spec_from_file_location("commands_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


commands = _load_commands_module()
CHANNEL = "@twothousanddotnews"


def _post(text, chat=None, key="channel_post"):
    return {
        "update_id": 1,
        key: {
            "message_id": 42,
            "chat": chat or {"id": -1001, "username": "twothousanddotnews", "type": "channel"},
            "text": text,
        },
    }


def test_bare_url_is_a_url_request():
    request = commands.parse(_post("  https://example.com/story?x=1\n"), CHANNEL)
    assert request.kind == "url"
    assert request.text == "https://example.com/story?x=1"
    assert (request.chat_id, request.message_id) == (-1001, 42)


def test_url_with_commentary_is_ignored():
    assert commands.parse(_post("look at this https://example.com/story"), CHANNEL) is None


def test_scoop_command_carries_its_query():
    request = commands.parse(_post("/scoop  taylor swift  "), CHANNEL)
    assert request.kind == "search"
    assert request.text == "taylor swift"


def test_scoop_addressed_to_the_bot_still_works():
    request = commands.parse(_post("/Scoop@twothousandbot climate summit"), CHANNEL)
    assert (request.kind, request.text) == ("search", "climate summit")


def test_bare_scoop_asks_for_usage():
    request = commands.parse(_post("/scoop"), CHANNEL)
    assert request.kind == "usage"


def test_scoop_prefix_of_another_word_is_not_a_command():
    assert commands.parse(_post("/scoops are great"), CHANNEL) is None


def test_ordinary_post_is_ignored():
    assert commands.parse(_post("Local Man Discovers Fire\n\nhttps://www.2000.news/x"), CHANNEL) is None


def test_other_chats_are_ignored():
    stranger = {"id": 555, "username": "someone", "type": "private"}
    assert commands.parse(_post("https://example.com", chat=stranger), CHANNEL) is None


def test_channel_can_be_configured_by_numeric_id():
    chat = {"id": -1001, "type": "channel"}
    assert commands.parse(_post("https://example.com", chat=chat), "-1001").kind == "url"
    assert commands.parse(_post("https://example.com", chat=chat), "-1002") is None


def test_direct_messages_are_ignored():
    assert commands.parse(_post("https://example.com", key="message"), CHANNEL) is None


def test_channel_username_matches_case_insensitively():
    chat = {"id": -1001, "username": "TwoThousandDotNews"}
    assert commands.parse(_post("https://example.com", chat=chat), CHANNEL).kind == "url"
