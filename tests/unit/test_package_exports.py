"""Unit tests for the package's public surface: ``src.__all__``."""

from __future__ import annotations

import pytest

import src as client
from src import wire

#: Names re-exported from ``.wire`` for ergonomics; the alias must be the original object.
WIRE_ALIASES = (
    "Message",
    "Header",
    "Properties",
    "ApplicationProperties",
    "MessageAnnotations",
    "DeliveryAnnotations",
    "Footer",
    "Data",
    "AmqpValue",
    "AmqpSequence",
    "Symbol",
    "Long",
    "Timestamp",
    "DeliveryState",
    "Accepted",
    "Rejected",
    "Released",
    "Modified",
    "Error",
)


class TestAll:
    @pytest.mark.parametrize("name", client.__all__)
    def test_every_exported_name_resolves(self, name):
        assert getattr(client, name, None) is not None

    def test_no_name_is_exported_twice(self):
        assert len(client.__all__) == len(set(client.__all__))


class TestWireAliases:
    @pytest.mark.parametrize("name", WIRE_ALIASES)
    def test_the_alias_is_the_wire_object(self, name):
        assert getattr(client, name) is getattr(wire, name)

    @pytest.mark.parametrize("name", WIRE_ALIASES)
    def test_the_alias_is_exported(self, name):
        assert name in client.__all__
