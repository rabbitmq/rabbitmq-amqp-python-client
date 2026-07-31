"""Exception hierarchy shared by every layer of the client."""

from __future__ import annotations


class AMQPError(Exception):
    """Base class for every error raised by this client."""


class ProtocolError(AMQPError):
    """The peer violated the AMQP 1.0 protocol or sent something unexpected."""


class AuthenticationError(AMQPError):
    """SASL negotiation was rejected by the broker."""


class AMQPTimeoutError(AMQPError):
    """A request did not receive a reply within its configured timeout."""


class InvalidAddressError(AMQPError):
    """A publisher/consumer address was configured inconsistently."""


class ValidationError(AMQPError, ValueError):
    """An argument was rejected locally, before any frame was sent.

    Also a :class:`ValueError`, so callers who only care that a value was
    rejected can catch the built-in type instead of importing this one.
    """


class PublisherError(AMQPError):
    """A publisher-specific operation failed."""


class ConsumerError(AMQPError):
    """A consumer-specific operation failed."""


class ManagementError(AMQPError):
    """A management-API request failed or returned an unexpected response."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
