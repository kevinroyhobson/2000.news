import importlib.util
import pathlib
import sys
import types


def _install_stubs():
    boto3 = types.ModuleType("boto3")
    boto3.client = lambda *args, **kwargs: None
    boto3.resource = lambda *args, **kwargs: types.SimpleNamespace(Table=lambda name: None)
    sys.modules.setdefault("boto3", boto3)
    lock = types.ModuleType("lib.tournament_lock")
    lib = types.ModuleType("lib")
    lib.tournament_lock = lock
    sys.modules.setdefault("lib", lib)
    sys.modules.setdefault("lib.tournament_lock", lock)


def _load_tournament_module():
    _install_stubs()
    path = pathlib.Path(__file__).resolve().parents[1] / "Tournament" / "tournament.py"
    spec = importlib.util.spec_from_file_location("tournament_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


tournament = _load_tournament_module()


def _record(deferred=None):
    image = {"HeadlineId": {"S": "abc"}}
    if deferred is not None:
        image["Deferred"] = {"BOOL": deferred}
    return {"eventName": "INSERT", "dynamodb": {"NewImage": image}}


def test_a_batch_of_only_deferred_headlines_starts_nothing():
    event = {"Records": [_record(True), _record(True)]}
    assert tournament.tournament(event, None) == "Deferred"


def test_one_scheduled_headline_in_the_batch_is_enough_to_run():
    assert not all(tournament._is_deferred(r) for r in [_record(True), _record()])
    assert not tournament._is_deferred(_record(False))
