"""
Unit tests for server validation functionality in the Connection class.
These tests use mocks to avoid external dependencies.

To run these tests:
    pytest tests/test_server_validation.py -v

Or to run a specific test:
    pytest tests/test_server_validation.py::TestServerValidation::test_validate_server_properties_success_exact_version
"""

from unittest.mock import Mock

import pytest

from rabbitmq_amqp_python_client.connection import (
    Connection,
)
from rabbitmq_amqp_python_client.exceptions import (
    ValidationCodeException,
)


class TestServerValidation:
    """Test cases for the _validate_server_properties method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.connection = Connection.__new__(Connection)
        # Initialize required attributes that would normally be set in __init__
        self.connection._conn = None
        self.connection._addr = "amqp://localhost:5672"
        self.connection._addrs = None
        self.connection._conf_ssl_context = None
        self.connection._managements = []
        self.connection._recovery_configuration = None
        self.connection._ssl_domain = None
        self.connection._connections = []
        self.connection._index = -1
        self.connection._publishers = []
        self.connection._consumers = []
        self.connection._oauth2_options = None

    def test_validate_server_properties_success_exact_version(self):
        """Test successful validation with exact minimum version 4.0.0."""
        # Create mock objects
        mock_blocking_conn = Mock()
        mock_proton_conn = Mock()
        mock_remote_props = {"product": "RabbitMQ", "version": "4.0.0"}

        mock_proton_conn.remote_properties = mock_remote_props
        mock_blocking_conn.conn = mock_proton_conn
        self.connection._conn = mock_blocking_conn

        # Should not raise any exception
        self.connection._validate_server_properties()

    def test_validate_server_properties_success_higher_version(self):
        """Test successful validation with version higher than minimum."""
        # Create mock objects
        mock_blocking_conn = Mock()
        mock_proton_conn = Mock()
        mock_remote_props = {"product": "RabbitMQ", "version": "4.1.2"}

        mock_proton_conn.remote_properties = mock_remote_props
        mock_blocking_conn.conn = mock_proton_conn
        self.connection._conn = mock_blocking_conn

        # Should not raise any exception
        self.connection._validate_server_properties()

    def test_validate_server_properties_no_connection(self):
        """Test validation fails when connection is None."""
        self.connection._conn = None

        with pytest.raises(ValidationCodeException) as exc_info:
            self.connection._validate_server_properties()

        assert "Connection not established" in str(exc_info.value)

    def test_validate_server_properties_no_proton_connection(self):
        """Test validation fails when proton connection is None."""
        mock_blocking_conn = Mock()
        mock_blocking_conn.conn = None
        self.connection._conn = mock_blocking_conn

        with pytest.raises(ValidationCodeException) as exc_info:
            self.connection._validate_server_properties()

        assert "Connection not established" in str(exc_info.value)

    def test_validate_server_properties_no_remote_properties(self):
        """Test validation fails when remote_properties is None."""
        mock_blocking_conn = Mock()
        mock_proton_conn = Mock()
        mock_proton_conn.remote_properties = None
        mock_blocking_conn.conn = mock_proton_conn
        self.connection._conn = mock_blocking_conn

        with pytest.raises(ValidationCodeException) as exc_info:
            self.connection._validate_server_properties()

        assert "No remote properties received from server" in str(exc_info.value)

    def test_validate_server_properties_wrong_product(self):
        """Test validation fails when server is not RabbitMQ."""
        mock_blocking_conn = Mock()
        mock_proton_conn = Mock()
        mock_remote_props = {"product": "Apache ActiveMQ", "version": "4.0.0"}

        mock_proton_conn.remote_properties = mock_remote_props
        mock_blocking_conn.conn = mock_proton_conn
        self.connection._conn = mock_blocking_conn

        with pytest.raises(ValidationCodeException) as exc_info:
            self.connection._validate_server_properties()

        error_msg = str(exc_info.value)
        assert "Connection to non-RabbitMQ server detected" in error_msg
        assert "Expected 'RabbitMQ', got 'Apache ActiveMQ'" in error_msg

    def test_validate_server_properties_missing_product(self):
        """Test validation fails when product property is missing."""
        mock_blocking_conn = Mock()
        mock_proton_conn = Mock()
        mock_remote_props = {
            "version": "4.0.0"
            # Missing "product" key
        }

        mock_proton_conn.remote_properties = mock_remote_props
        mock_blocking_conn.conn = mock_proton_conn
        self.connection._conn = mock_blocking_conn

        with pytest.raises(ValidationCodeException) as exc_info:
            self.connection._validate_server_properties()

        error_msg = str(exc_info.value)
        assert "Connection to non-RabbitMQ server detected" in error_msg
        assert "Expected 'RabbitMQ', got 'None'" in error_msg

    def test_validate_server_properties_missing_version(self):
        """Test validation fails when version property is missing."""
        mock_blocking_conn = Mock()
        mock_proton_conn = Mock()
        mock_remote_props = {
            "product": "RabbitMQ"
            # Missing "version" key
        }

        mock_proton_conn.remote_properties = mock_remote_props
        mock_blocking_conn.conn = mock_proton_conn
        self.connection._conn = mock_blocking_conn

        with pytest.raises(ValidationCodeException) as exc_info:
            self.connection._validate_server_properties()

        assert "Server version not provided" in str(exc_info.value)

    def test_validate_server_properties_version_too_low(self):
        """Test validation fails when server version is below 4.0.0."""
        test_cases = ["3.9.9", "3.12.0", "2.8.7", "1.0.0"]

        for version_str in test_cases:
            mock_blocking_conn = Mock()
            mock_proton_conn = Mock()
            mock_remote_props = {"product": "RabbitMQ", "version": version_str}

            mock_proton_conn.remote_properties = mock_remote_props
            mock_blocking_conn.conn = mock_proton_conn
            self.connection._conn = mock_blocking_conn

            with pytest.raises(ValidationCodeException) as exc_info:
                self.connection._validate_server_properties()

            error_msg = str(exc_info.value)
            assert (
                "The AMQP client library requires RabbitMQ 4.0.0 or higher" in error_msg
            )
            assert f"Server version: {version_str}" in error_msg

    def test_validate_server_properties_valid_higher_versions(self):
        """Test validation succeeds with various higher versions."""
        valid_versions = [
            "4.0.0",
            "4.0.1",
            "4.1.0",
            "4.10.15",
            "5.0.0",
            "10.2.3",
            "v4.0.0",  # v prefix should be stripped and accepted
            "4.0.0.0.0",  # Extra zeroes should be normalized and accepted
        ]

        for version_str in valid_versions:
            mock_blocking_conn = Mock()
            mock_proton_conn = Mock()
            mock_remote_props = {"product": "RabbitMQ", "version": version_str}

            mock_proton_conn.remote_properties = mock_remote_props
            mock_blocking_conn.conn = mock_proton_conn
            self.connection._conn = mock_blocking_conn

            # Should not raise any exception
            self.connection._validate_server_properties()

    def test_validate_server_properties_invalid_version_format(self):
        """Test validation handles invalid version formats gracefully."""
        invalid_versions = [
            "invalid-version",
            "4.0.0-alpha",  # Pre-release, should be rejected
            "",
        ]

        for version_str in invalid_versions:
            mock_blocking_conn = Mock()
            mock_proton_conn = Mock()
            mock_remote_props = {"product": "RabbitMQ", "version": version_str}

            mock_proton_conn.remote_properties = mock_remote_props
            mock_blocking_conn.conn = mock_proton_conn
            self.connection._conn = mock_blocking_conn

            with pytest.raises(ValidationCodeException) as exc_info:
                self.connection._validate_server_properties()

            error_msg = str(exc_info.value)
            # Should either be version too low or parsing error
            assert (
                "Failed to parse server version" in error_msg
                or "requires RabbitMQ 4.0.0 or higher" in error_msg
            )

    def test_validate_server_properties_version_edge_cases(self):
        """Test validation with edge case version values."""
        # Test with pre-release versions that should still work
        edge_case_versions = [
            "4.0.0-rc1",  # This might work depending on packaging library
            "4.0.0b1",  # Beta version
        ]

        for version_str in edge_case_versions:
            mock_blocking_conn = Mock()
            mock_proton_conn = Mock()
            mock_remote_props = {"product": "RabbitMQ", "version": version_str}

            mock_proton_conn.remote_properties = mock_remote_props
            mock_blocking_conn.conn = mock_proton_conn
            self.connection._conn = mock_blocking_conn

            try:
                # Depending on packaging library behavior, this might pass or fail
                # The important thing is that it doesn't crash
                self.connection._validate_server_properties()
            except ValidationCodeException:
                # This is acceptable for edge cases
                pass

    def test_validate_server_properties_with_additional_properties(self):
        """Test validation works when remote_properties has additional fields."""
        mock_blocking_conn = Mock()
        mock_proton_conn = Mock()
        mock_remote_props = {
            "product": "RabbitMQ",
            "version": "4.2.1",
            "platform": "Linux",
            "information": "Licensed under the MPL 2.0",
            "copyright": "Copyright (c) 2007-2023 VMware, Inc. or its affiliates.",
        }

        mock_proton_conn.remote_properties = mock_remote_props
        mock_blocking_conn.conn = mock_proton_conn
        self.connection._conn = mock_blocking_conn

        # Should not raise any exception despite additional properties
        self.connection._validate_server_properties()


class TestServerVersionGte420:
    """Test cases for the _is_server_version_gte_4_2_0 method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.connection = Connection.__new__(Connection)
        # Initialize required attributes that would normally be set in __init__
        self.connection._conn = None
        self.connection._addr = "amqp://localhost:5672"
        self.connection._addrs = None
        self.connection._conf_ssl_context = None
        self.connection._managements = []
        self.connection._recovery_configuration = None
        self.connection._ssl_domain = None
        self.connection._connections = []
        self.connection._index = -1
        self.connection._publishers = []
        self.connection._consumers = []
        self.connection._oauth2_options = None

    def test_is_server_version_gte_4_2_0_exact_version(self):
        """Test with exact version 4.2.0."""
        mock_blocking_conn = Mock()
        mock_proton_conn = Mock()
        mock_remote_props = {"product": "RabbitMQ", "version": "4.2.0"}

        mock_proton_conn.remote_properties = mock_remote_props
        mock_blocking_conn.conn = mock_proton_conn
        self.connection._conn = mock_blocking_conn

        result = self.connection._is_server_version_gte_4_2_0()
        assert result is True

    def test_is_server_version_gte_4_2_0_higher_versions(self):
        """Test with versions higher than 4.2.0."""
        higher_versions = ["4.2.1", "4.3.0", "4.10.5", "5.0.0", "6.1.2"]

        for version_str in higher_versions:
            mock_blocking_conn = Mock()
            mock_proton_conn = Mock()
            mock_remote_props = {"product": "RabbitMQ", "version": version_str}

            mock_proton_conn.remote_properties = mock_remote_props
            mock_blocking_conn.conn = mock_proton_conn
            self.connection._conn = mock_blocking_conn

            result = self.connection._is_server_version_gte_4_2_0()
            assert result is True, f"Version {version_str} should return True"

    def test_is_server_version_gte_4_2_0_lower_versions(self):
        """Test with versions lower than 4.2.0."""
        lower_versions = ["4.1.9", "4.1.0", "4.0.0", "3.12.0", "3.9.5"]

        for version_str in lower_versions:
            mock_blocking_conn = Mock()
            mock_proton_conn = Mock()
            mock_remote_props = {"product": "RabbitMQ", "version": version_str}

            mock_proton_conn.remote_properties = mock_remote_props
            mock_blocking_conn.conn = mock_proton_conn
            self.connection._conn = mock_blocking_conn

            result = self.connection._is_server_version_gte_4_2_0()
            assert result is False, f"Version {version_str} should return False"

    def test_is_server_version_gte_4_2_0_no_connection(self):
        """Test when connection is None."""
        self.connection._conn = None

        with pytest.raises(ValidationCodeException) as exc_info:
            self.connection._is_server_version_gte_4_2_0()

        assert "Connection not established" in str(exc_info.value)

    def test_is_server_version_gte_4_2_0_no_proton_connection(self):
        """Test when proton connection is None."""
        mock_blocking_conn = Mock()
        mock_blocking_conn.conn = None
        self.connection._conn = mock_blocking_conn

        with pytest.raises(ValidationCodeException) as exc_info:
            self.connection._is_server_version_gte_4_2_0()

        assert "Connection not established" in str(exc_info.value)

    def test_is_server_version_gte_4_2_0_no_remote_properties(self):
        """Test when remote_properties is None."""
        mock_blocking_conn = Mock()
        mock_proton_conn = Mock()
        mock_proton_conn.remote_properties = None
        mock_blocking_conn.conn = mock_proton_conn
        self.connection._conn = mock_blocking_conn

        with pytest.raises(ValidationCodeException) as exc_info:
            self.connection._is_server_version_gte_4_2_0()

        assert "No remote properties received from server" in str(exc_info.value)

    def test_is_server_version_gte_4_2_0_missing_version(self):
        """Test when version property is missing."""
        mock_blocking_conn = Mock()
        mock_proton_conn = Mock()
        mock_remote_props = {
            "product": "RabbitMQ"
            # Missing "version" key
        }

        mock_proton_conn.remote_properties = mock_remote_props
        mock_blocking_conn.conn = mock_proton_conn
        self.connection._conn = mock_blocking_conn

        with pytest.raises(ValidationCodeException) as exc_info:
            self.connection._is_server_version_gte_4_2_0()

        assert "Server version not provided" in str(exc_info.value)

    def test_is_server_version_gte_4_2_0_invalid_version_format(self):
        """Test with invalid version formats."""
        invalid_versions = ["invalid-version", "", "not-a-version"]

        for version_str in invalid_versions:
            mock_blocking_conn = Mock()
            mock_proton_conn = Mock()
            mock_remote_props = {"product": "RabbitMQ", "version": version_str}

            mock_proton_conn.remote_properties = mock_remote_props
            mock_blocking_conn.conn = mock_proton_conn
            self.connection._conn = mock_blocking_conn

            with pytest.raises(ValidationCodeException) as exc_info:
                self.connection._is_server_version_gte_4_2_0()

            error_msg = str(exc_info.value)
            assert "Failed to parse server version" in error_msg

    def test_is_server_version_gte_4_2_0_edge_cases(self):
        """Test with edge case versions."""
        # Test cases around the boundary
        test_cases = [
            ("4.1.99", False),  # Just below
            ("4.2.0", True),  # Exact match
            ("4.2.0.0", True),  # With extra zeroes
            ("v4.2.0", True),  # With v prefix
            ("4.2.0-rc1", False),  # Pre-release should be less than 4.2.0
        ]

        for version_str, expected in test_cases:
            mock_blocking_conn = Mock()
            mock_proton_conn = Mock()
            mock_remote_props = {"product": "RabbitMQ", "version": version_str}

            mock_proton_conn.remote_properties = mock_remote_props
            mock_blocking_conn.conn = mock_proton_conn
            self.connection._conn = mock_blocking_conn

            if version_str == "4.2.0-rc1":
                # Pre-release versions should be handled correctly
                result = self.connection._is_server_version_gte_4_2_0()
                assert (
                    result == expected
                ), f"Version {version_str} should return {expected}"
            else:
                result = self.connection._is_server_version_gte_4_2_0()
                assert (
                    result == expected
                ), f"Version {version_str} should return {expected}"
