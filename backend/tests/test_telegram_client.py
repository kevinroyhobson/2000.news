import email.parser
import importlib.util
import pathlib
import sys
import types
from unittest import mock

BACKEND = pathlib.Path(__file__).resolve().parents[1]


def _load_telegram_module():
    spec = importlib.util.spec_from_file_location("telegram_module", BACKEND / "lib" / "telegram.py")
    module = importlib.util.module_from_spec(spec)
    ssm_secrets = types.ModuleType("lib.ssm_secrets")
    ssm_secrets.get_secret = None
    with mock.patch.dict(sys.modules, {"lib.ssm_secrets": ssm_secrets}):
        spec.loader.exec_module(module)
    return module


telegram = _load_telegram_module()


def _parse_form(body, content_type):
    message = email.parser.BytesParser().parsebytes(
        f"Content-Type: {content_type}\r\n\r\n".encode() + body)
    return {part.get_param("name", header="content-disposition"): part for part in message.get_payload()}


def test_multipart_form_round_trips_fields_and_file():
    jpeg = b"\xff\xd8\xff\xe0 not really a jpeg \r\n--boundary-lookalike\r\n\xff\xd9"
    body, content_type = telegram.multipart_form(
        {"chat_id": "-100", "caption": "Wife’s Tumor Benign\nhttps://www.2000.news/20261021/5c31ed2b"},
        "document", "20261021-5c31ed2b.jpg", jpeg)

    parts = _parse_form(body, content_type)

    assert parts["chat_id"].get_payload(decode=True) == b"-100"
    assert parts["caption"].get_payload(decode=True).decode() == (
        "Wife’s Tumor Benign\nhttps://www.2000.news/20261021/5c31ed2b")
    document = parts["document"]
    assert document.get_filename() == "20261021-5c31ed2b.jpg"
    assert document.get_content_type() == "image/jpeg"
    assert document.get_payload(decode=True) == jpeg
