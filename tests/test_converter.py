import pytest

from rabbitmq_amqp_python_client.utils import Converter


def test_bytes_to_string_ascii() -> None:
    assert Converter.bytes_to_string(b"hello") == "hello"


def test_bytes_to_string_non_ascii_latin() -> None:
    # Two-byte UTF-8 sequences: é, ü, ñ, etc.
    text = "café naïve résumé"
    assert Converter.bytes_to_string(text.encode("utf-8")) == text


def test_bytes_to_string_non_ascii_cyrillic() -> None:
    text = "Привет мир"
    assert Converter.bytes_to_string(text.encode("utf-8")) == text


def test_bytes_to_string_non_ascii_cjk() -> None:
    # Three-byte UTF-8 sequences: CJK ideographs
    text = "你好世界"
    assert Converter.bytes_to_string(text.encode("utf-8")) == text


def test_bytes_to_string_emoji() -> None:
    # Four-byte UTF-8 sequences
    text = "Hello 🐇 RabbitMQ"
    assert Converter.bytes_to_string(text.encode("utf-8")) == text


def test_bytes_to_string_custom_encoding() -> None:
    text = "café"
    assert Converter.bytes_to_string(text.encode("latin-1"), encoding="latin-1") == text


def test_bytes_to_string_invalid_encoding_raises() -> None:
    with pytest.raises(UnicodeDecodeError):
        # Valid UTF-8 bytes interpreted as ASCII (strict) will raise for non-ASCII bytes
        Converter.bytes_to_string("café".encode("utf-8"), encoding="ascii")


def test_bytes_to_string_empty() -> None:
    assert Converter.bytes_to_string(b"") == ""


def test_string_to_bytes_and_back_ascii() -> None:
    text = "simple ascii"
    assert Converter.bytes_to_string(Converter.string_to_bytes(text)) == text


def test_string_to_bytes_and_back_non_ascii() -> None:
    text = "こんにちは"
    assert Converter.bytes_to_string(Converter.string_to_bytes(text)) == text
