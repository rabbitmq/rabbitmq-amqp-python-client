"""
Unit tests for AmqpUri and the amqp_uri parameter on Environment / AsyncEnvironment.

These tests are pure unit tests and do not require a running RabbitMQ broker.
"""

import pytest

from rabbitmq_amqp_python_client import (
    AmqpUri,
    AsyncEnvironment,
    Environment,
)


class TestAmqpUri:
    """Tests for the AmqpUri dataclass and its to_uri() helper."""

    def test_default_values_produce_standard_uri(self) -> None:
        uri = AmqpUri()
        assert uri.to_uri() == "amqp://guest:guest@localhost:5672/"

    def test_custom_host(self) -> None:
        uri = AmqpUri(host="rabbit.example.com")
        assert uri.to_uri() == "amqp://guest:guest@rabbit.example.com:5672/"

    def test_custom_port(self) -> None:
        uri = AmqpUri(port=5673)
        assert uri.to_uri() == "amqp://guest:guest@localhost:5673/"

    def test_custom_schema_amqps(self) -> None:
        uri = AmqpUri(schema="amqps", port=5671)
        assert uri.to_uri() == "amqps://guest:guest@localhost:5671/"

    def test_custom_user_and_password(self) -> None:
        uri = AmqpUri(user="admin", password="s3cr3t")
        assert uri.to_uri() == "amqp://admin:s3cr3t@localhost:5672/"

    def test_default_vhost_slash_produces_single_slash_path(self) -> None:
        uri = AmqpUri(vhost="/")
        assert uri.to_uri().endswith("/")
        # path component should be exactly "/"
        assert uri.to_uri() == "amqp://guest:guest@localhost:5672/"

    def test_custom_vhost(self) -> None:
        uri = AmqpUri(vhost="production")
        assert uri.to_uri() == "amqp://guest:guest@localhost:5672/production"

    def test_special_chars_in_password_are_percent_encoded(self) -> None:
        uri = AmqpUri(password="p@ss:w/ord")
        result = uri.to_uri()
        # "@", ":", "/" must be percent-encoded in the password
        assert "p%40ss%3Aw%2Ford" in result

    def test_special_chars_in_user_are_percent_encoded(self) -> None:
        uri = AmqpUri(user="user@domain")
        result = uri.to_uri()
        assert "user%40domain" in result

    def test_special_chars_in_vhost_are_percent_encoded(self) -> None:
        uri = AmqpUri(vhost="my/vhost")
        result = uri.to_uri()
        assert result == "amqp://guest:guest@localhost:5672/my%2Fvhost"

    def test_all_custom_fields(self) -> None:
        uri = AmqpUri(
            schema="amqps",
            host="broker.internal",
            port=5671,
            user="alice",
            password="wonderland",
            vhost="staging",
        )
        assert uri.to_uri() == "amqps://alice:wonderland@broker.internal:5671/staging"

    def test_dataclass_field_defaults(self) -> None:
        uri = AmqpUri()
        assert uri.schema == "amqp"
        assert uri.host == "localhost"
        assert uri.port == 5672
        assert uri.user == "guest"
        assert uri.password == "guest"
        assert uri.vhost == "/"

    def test_dataclass_fields_are_mutable(self) -> None:
        uri = AmqpUri()
        uri.host = "other-host"
        uri.port = 9999
        assert uri.to_uri() == "amqp://guest:guest@other-host:9999/"


class TestEnvironmentAmqpUri:
    """Tests for Environment.__init__ with the amqp_uri parameter."""

    # ------------------------------------------------------------------
    # Happy-path: each of the three modes accepted individually
    # ------------------------------------------------------------------

    def test_amqp_uri_is_accepted(self) -> None:
        env = Environment(amqp_uri=AmqpUri())
        assert env._uri == "amqp://guest:guest@localhost:5672/"
        assert env._uris is None

    def test_amqp_uri_converted_correctly(self) -> None:
        amqp_uri = AmqpUri(host="mybroker", port=5673, user="bob", password="pw")
        env = Environment(amqp_uri=amqp_uri)
        assert env._uri == "amqp://bob:pw@mybroker:5673/"

    def test_uri_string_still_accepted(self) -> None:
        env = Environment(uri="amqp://guest:guest@localhost:5672/")
        assert env._uri == "amqp://guest:guest@localhost:5672/"
        assert env._uris is None

    def test_uris_list_still_accepted(self) -> None:
        uris = ["amqp://guest:guest@node1:5672/", "amqp://guest:guest@node2:5672/"]
        env = Environment(uris=uris)
        assert env._uris == uris
        assert env._uri is None

    # ------------------------------------------------------------------
    # Error cases: more than one mode specified
    # ------------------------------------------------------------------

    def test_uri_and_amqp_uri_together_raises(self) -> None:
        with pytest.raises(ValueError, match="amqp_uri"):
            Environment(uri="amqp://guest:guest@localhost:5672/", amqp_uri=AmqpUri())

    def test_uris_and_amqp_uri_together_raises(self) -> None:
        with pytest.raises(ValueError, match="amqp_uri"):
            Environment(uris=["amqp://guest:guest@localhost:5672/"], amqp_uri=AmqpUri())

    def test_uri_and_uris_together_raises(self) -> None:
        with pytest.raises(ValueError, match="amqp_uri"):
            Environment(
                uri="amqp://guest:guest@localhost:5672/",
                uris=["amqp://guest:guest@localhost:5672/"],
            )

    def test_all_three_together_raises(self) -> None:
        with pytest.raises(ValueError, match="amqp_uri"):
            Environment(
                uri="amqp://guest:guest@localhost:5672/",
                uris=["amqp://guest:guest@localhost:5672/"],
                amqp_uri=AmqpUri(),
            )

    def test_none_provided_raises(self) -> None:
        with pytest.raises(ValueError, match="amqp_uri"):
            Environment()

    # ------------------------------------------------------------------
    # Custom AmqpUri fields are reflected in the stored URI
    # ------------------------------------------------------------------

    def test_amqp_uri_vhost_reflected_in_stored_uri(self) -> None:
        env = Environment(amqp_uri=AmqpUri(vhost="myvhost"))
        assert env._uri == "amqp://guest:guest@localhost:5672/myvhost"

    def test_amqp_uri_default_vhost_reflected_in_stored_uri(self) -> None:
        env = Environment(amqp_uri=AmqpUri(vhost="/"))
        assert env._uri == "amqp://guest:guest@localhost:5672/"


class TestAsyncEnvironmentAmqpUri:
    """Tests for AsyncEnvironment.__init__ with the amqp_uri parameter."""

    # ------------------------------------------------------------------
    # Happy-path
    # ------------------------------------------------------------------

    def test_amqp_uri_is_accepted(self) -> None:
        env = AsyncEnvironment(amqp_uri=AmqpUri())
        assert env._uri == "amqp://guest:guest@localhost:5672/"
        assert env._uris is None

    def test_amqp_uri_converted_correctly(self) -> None:
        amqp_uri = AmqpUri(host="asyncbroker", port=5674, user="carol", password="xyz")
        env = AsyncEnvironment(amqp_uri=amqp_uri)
        assert env._uri == "amqp://carol:xyz@asyncbroker:5674/"

    def test_uri_string_still_accepted(self) -> None:
        env = AsyncEnvironment(uri="amqp://guest:guest@localhost:5672/")
        assert env._uri == "amqp://guest:guest@localhost:5672/"
        assert env._uris is None

    def test_uris_list_still_accepted(self) -> None:
        uris = ["amqp://guest:guest@node1:5672/", "amqp://guest:guest@node2:5672/"]
        env = AsyncEnvironment(uris=uris)
        assert env._uris == uris
        assert env._uri is None

    # ------------------------------------------------------------------
    # Error cases
    # ------------------------------------------------------------------

    def test_uri_and_amqp_uri_together_raises(self) -> None:
        with pytest.raises(ValueError, match="amqp_uri"):
            AsyncEnvironment(
                uri="amqp://guest:guest@localhost:5672/", amqp_uri=AmqpUri()
            )

    def test_uris_and_amqp_uri_together_raises(self) -> None:
        with pytest.raises(ValueError, match="amqp_uri"):
            AsyncEnvironment(
                uris=["amqp://guest:guest@localhost:5672/"], amqp_uri=AmqpUri()
            )

    def test_uri_and_uris_together_raises(self) -> None:
        with pytest.raises(ValueError, match="amqp_uri"):
            AsyncEnvironment(
                uri="amqp://guest:guest@localhost:5672/",
                uris=["amqp://guest:guest@localhost:5672/"],
            )

    def test_all_three_together_raises(self) -> None:
        with pytest.raises(ValueError, match="amqp_uri"):
            AsyncEnvironment(
                uri="amqp://guest:guest@localhost:5672/",
                uris=["amqp://guest:guest@localhost:5672/"],
                amqp_uri=AmqpUri(),
            )

    def test_none_provided_raises(self) -> None:
        with pytest.raises(ValueError, match="amqp_uri"):
            AsyncEnvironment()

    # ------------------------------------------------------------------
    # Custom AmqpUri fields
    # ------------------------------------------------------------------

    def test_amqp_uri_vhost_reflected_in_stored_uri(self) -> None:
        env = AsyncEnvironment(amqp_uri=AmqpUri(vhost="staging"))
        assert env._uri == "amqp://guest:guest@localhost:5672/staging"

    def test_amqp_uri_default_vhost_reflected_in_stored_uri(self) -> None:
        env = AsyncEnvironment(amqp_uri=AmqpUri(vhost="/"))
        assert env._uri == "amqp://guest:guest@localhost:5672/"
