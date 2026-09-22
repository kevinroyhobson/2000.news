import importlib.util
import pathlib


def _load_reactions_module():
    path = pathlib.Path(__file__).resolve().parents[1] / "TelegramReaction" / "reactions.py"
    spec = importlib.util.spec_from_file_location("reactions_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


reactions = _load_reactions_module()


def _count_update(*emoji_counts, chat_id=-1001, message_id=42):
    """A channel-style anonymous update: aggregate counts, no user."""
    return {
        "update_id": 1,
        "message_reaction_count": {
            "chat": {"id": chat_id},
            "message_id": message_id,
            "reactions": [
                {"type": {"type": "emoji", "emoji": emoji}, "total_count": count}
                for emoji, count in emoji_counts
            ],
        },
    }


def _user_update(old, new, chat_id=-1001, message_id=42):
    return {
        "update_id": 1,
        "message_reaction": {
            "chat": {"id": chat_id},
            "message_id": message_id,
            "user": {"id": 777},
            "old_reaction": [{"type": "emoji", "emoji": e} for e in old],
            "new_reaction": [{"type": "emoji", "emoji": e} for e in new],
        },
    }


def test_fire_on_a_channel_post_grades_outstanding():
    result = reactions.parse(_count_update(("🔥", 1)))

    assert result.grade == "outstanding"
    assert result.chat_id == -1001
    assert result.message_id == 42


def test_thumbs_up_and_laughing_grade_solid():
    assert reactions.parse(_count_update(("👍", 1))).grade == "solid"
    assert reactions.parse(_count_update(("🤣", 1))).grade == "solid"


def test_thumbs_down_grades_bad():
    assert reactions.parse(_count_update(("👎", 1))).grade == "bad"


def test_emoji_match_with_or_without_the_variation_selector():
    assert reactions.parse(_count_update(("🔥️", 1))).grade == "outstanding"


def test_most_reacted_emoji_wins():
    result = reactions.parse(_count_update(("🔥", 1), ("👎", 3)))

    assert result.grade == "bad"


def test_tied_counts_break_toward_the_stronger_opinion():
    result = reactions.parse(_count_update(("🔥", 2), ("👍", 2)))

    assert result.grade == "outstanding"


def test_unmapped_and_custom_emoji_are_ignored():
    update = _count_update(("🦄", 5))
    update["message_reaction_count"]["reactions"].append(
        {"type": {"type": "custom_emoji", "custom_emoji_id": "123"}, "total_count": 9}
    )

    assert reactions.parse(update).grade is None


def test_removing_the_last_grading_reaction_is_an_undo():
    assert reactions.parse(_count_update()).grade is None


def test_identified_reaction_uses_the_new_reaction_list():
    result = reactions.parse(_user_update([], ["🔥"]))

    assert result.grade == "outstanding"


def test_identified_undo_only_counts_when_a_grade_was_removed():
    undone = reactions.parse(_user_update(["👍"], []))
    unrelated = reactions.parse(_user_update(["🦄"], ["🙈"]))

    assert undone.grade is None
    assert unrelated is None


def test_non_reaction_updates_are_ignored():
    assert reactions.parse({"update_id": 1, "message": {"text": "hi"}}) is None


def test_every_mapped_emoji_is_a_known_grade():
    assert set(reactions.EMOJI_GRADES.values()) <= set(reactions.GRADE_PRECEDENCE)
