import importlib.util
import pathlib


def _load_topic_search_module():
    path = pathlib.Path(__file__).resolve().parents[1] / "lib" / "topic_search.py"
    spec = importlib.util.spec_from_file_location("topic_search_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


topic_search = _load_topic_search_module()


def test_plain_topic_requires_every_word():
    assert topic_search.require_every_word("cincinnati  reds ") == "cincinnati AND reds"


def test_single_word_is_unchanged():
    assert topic_search.require_every_word("bengals") == "bengals"


def test_quoted_phrase_is_sent_as_written():
    assert topic_search.require_every_word('"joe burrow" injury') == '"joe burrow" injury'


def test_explicit_operators_are_sent_as_written():
    assert topic_search.require_every_word("bengals OR browns") == "bengals OR browns"
    assert topic_search.require_every_word("reds NOT baseball") == "reds NOT baseball"


def test_lowercase_words_that_look_like_operators_are_just_words():
    assert topic_search.require_every_word("black or white") == "black AND or AND white"
