import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote

import pytest

from rabbitmq_amqp_python_client import AddressHelper, address_helper
from rabbitmq_amqp_python_client.address_helper import (
    ENCODER_ENV_VAR,
    ENCODER_LEGACY,
    ENCODER_QUOTE,
    _encode_path_segment_legacy,
    _encode_path_segment_quote,
)


def test_encoding_queue_simple() -> None:
    queue = "my_queue"

    address = AddressHelper.queue_address(queue)

    assert address == "/queues/my_queue"


def test_encoding_queue_hex() -> None:
    queue = "my_queue>"

    address = AddressHelper.queue_address(queue)

    assert address == "/queues/my_queue%3E"


def test_encoding_exchange_hex() -> None:
    queue = "my_exchange/()"

    address = AddressHelper.exchange_address(queue)

    assert address == "/exchanges/my_exchange%2F%28%29"


# --- O1: A/B tests legacy vs quote ----------------------------------------

ASCII_CORPUS = [
    "ordini-2026",
    "my_queue",
    "my_queue>",
    "my_exchange/()",
    "code con spazi",
    "a/b?c#d;e=f&g",
    "q" * 64,
    "".join(chr(code) for code in range(32, 127)),
]


@pytest.mark.parametrize("value", ASCII_CORPUS)
def test_quote_encoder_matches_legacy_on_ascii(value: str) -> None:
    """No regression on ASCII names: byte-for-byte identical output."""
    assert _encode_path_segment_quote(value) == _encode_path_segment_legacy(value)


@pytest.mark.parametrize("value", ["città/italia", "emoji-🚀-queue", "日本語"]) 
def test_quote_encoder_uses_utf8_percent_encoding(value: str) -> None:
    encoded = _encode_path_segment_quote(value)

    assert unquote(encoded) == value
    assert all(
        ord(char) < 128 for char in encoded
    ), "the URI must contain only ASCII characters"


def test_quote_encoder_unicode_examples() -> None:
    assert _encode_path_segment_quote("città/italia") == "citt%C3%A0%2Fitalia"
    assert _encode_path_segment_quote("emoji-🚀-queue") == "emoji-%F0%9F%9A%80-queue"


def test_legacy_encoder_characterisation() -> None:
    """Current behaviour, documented: the "legacy" flag does not change it."""
    assert _encode_path_segment_legacy("città") == "città"
    assert _encode_path_segment_legacy("🚀") == "%1F680"


@pytest.mark.parametrize("encoder", [_encode_path_segment_legacy, _encode_path_segment_quote])
def test_encoders_handle_none_and_empty(encoder) -> None:
    assert encoder(None) == ""
    assert encoder("") == ""


def test_flag_defaults_to_legacy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENCODER_ENV_VAR, raising=False)
    assert address_helper._resolve_encoder() == ENCODER_LEGACY


def test_flag_accepts_quote_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENCODER_ENV_VAR, " QUOTE ")
    assert address_helper._resolve_encoder() == ENCODER_QUOTE


def test_flag_rejects_unknown_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENCODER_ENV_VAR, "fast")
    with pytest.raises(ValueError, match=ENCODER_ENV_VAR):
        address_helper._resolve_encoder()


def test_public_function_honours_mode() -> None:
    original = address_helper._ENCODER
    try:
        address_helper._set_encoder(ENCODER_LEGACY)
        assert AddressHelper.queue_address("città") == "/queues/città"

        address_helper._set_encoder(ENCODER_QUOTE)
        assert AddressHelper.queue_address("città") == "/queues/citt%C3%A0"
    finally:
        address_helper._set_encoder(original)


def test_set_encoder_rejects_unknown() -> None:
    with pytest.raises(ValueError):
        address_helper._set_encoder("fast")


def _run_in_subprocess(env_value: str, code: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env[ENCODER_ENV_VAR] = env_value
    root = Path(__file__).resolve().parents[1]
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=env,
        cwd=root,
    )


def test_flag_is_read_at_import() -> None:
    code = (
        "from rabbitmq_amqp_python_client import AddressHelper; "
        "print(AddressHelper.queue_address('città'))"
    )

    precedent = _run_in_subprocess(ENCODER_LEGACY, code)
    assert precedent.returncode == 0, precedent.stderr
    assert precedent.stdout.strip() == "/queues/città"

    enabled = _run_in_subprocess(ENCODER_QUOTE, code)
    assert enabled.returncode == 0, enabled.stderr
    assert enabled.stdout.strip() == "/queues/citt%C3%A0"


def test_invalid_flag_fails_at_import() -> None:
    code = "import rabbitmq_amqp_python_client.address_helper"

    result = _run_in_subprocess("fast", code)
    assert result.returncode != 0
    assert ENCODER_ENV_VAR in result.stderr
