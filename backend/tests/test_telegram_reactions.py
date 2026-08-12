import importlib.util
import pathlib


def _load_reactions_module():
    path = pathlib.Path(__file__).resolve().parents[1] / "TelegramReaction" / "reactions.py"
    spec = importlib.util.spec_from_file_location("reactions_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


reactions = _load_reactions_module()
GRADES = reactions.DEFAULT_EMOJI_GRADES


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


def _user_update(old, new, user_id=777, chat_id=-1001, message_id=42):
    return {
        "update_id": 1,
        "message_reaction": {
            "chat": {"id": chat_id},
            "message_id": message_id,
            "user": {"id": user_id},
            "old_reaction": [{"type": "emoji", "emoji": e} for e in old],
            "new_reaction": [{"type": "emoji", "emoji": e} for e in new],
        },
    }


def test_trophy_on_a_channel_post_grades_outstanding():
    result = reactions.parse(_count_update(("🏆", 1)), GRADES, set())

    assert result.grade == "outstanding"
    assert result.chat_id == -1001
    assert result.message_id == 42


def test_heart_matches_with_or_without_the_variation_selector():
    with_selector = reactions.parse(_count_update(("❤️", 1)), GRADES, set())
    without = reactions.parse(_count_update(("❤", 1)), GRADES, set())

    assert with_selector.grade == "outstanding"
    assert without.grade == "outstanding"


def test_thumbs_down_grades_bad():
    assert reactions.parse(_count_update(("👎", 1)), GRADES, set()).grade == "bad"


def test_most_reacted_emoji_wins():
    result = reactions.parse(_count_update(("🏆", 1), ("👎", 3)), GRADES, set())

    assert result.grade == "bad"


def test_tied_counts_break_toward_the_stronger_opinion():
    result = reactions.parse(_count_update(("🏆", 2), ("😐", 2)), GRADES, set())

    assert result.grade == "outstanding"


def test_unmapped_and_custom_emoji_are_ignored():
    update = _count_update(("🦄", 5))
    update["message_reaction_count"]["reactions"].append(
        {"type": {"type": "custom_emoji", "custom_emoji_id": "123"}, "total_count": 9}
    )

    assert reactions.parse(update, GRADES, set()).grade is None


def test_removing_the_last_grading_reaction_is_an_undo():
    assert reactions.parse(_count_update(), GRADES, set()).grade is None


def test_identified_reaction_uses_the_new_reaction_list():
    result = reactions.parse(_user_update([], ["🏆"]), GRADES, set())

    assert result.grade == "outstanding"


def test_identified_undo_only_counts_when_a_grade_was_removed():
    undone = reactions.parse(_user_update(["🏆"], []), GRADES, set())
    unrelated = reactions.parse(_user_update(["🦄"], ["🙈"]), GRADES, set())

    assert undone.grade is None
    assert unrelated is None


def test_allowlist_rejects_other_users_and_anonymous_taps():
    outsider = reactions.parse(_user_update([], ["🏆"], user_id=999), GRADES, {777})
    anonymous = reactions.parse(_count_update(("🏆", 1)), GRADES, {777})
    allowed = reactions.parse(_user_update([], ["🏆"], user_id=777), GRADES, {777})

    assert outsider is None
    assert anonymous is None
    assert allowed.grade == "outstanding"


def test_non_reaction_updates_are_ignored():
    assert reactions.parse({"update_id": 1, "message": {"text": "hi"}}, GRADES, set()) is None


def test_every_mapped_emoji_is_a_known_grade():
    assert set(GRADES.values()) <= set(reactions.GRADE_PRECEDENCE)
