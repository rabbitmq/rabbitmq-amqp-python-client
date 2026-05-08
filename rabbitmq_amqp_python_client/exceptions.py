class ValidationCodeException(BaseException):
    # Constructor or Initializer
    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self) -> str:
        return repr(self.msg)


class ArgumentOutOfRangeException(BaseException):
    # Constructor or Initializer
    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self) -> str:
        return repr(self.msg)


class InvalidOperationException(Exception):
    # Constructor or Initializer
    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self) -> str:
        return repr(self.msg)


class AmqpMessageRejectedException(Exception):
    """Exception raised when a published message is rejected by the broker.

    The exception message contains the rejection reason provided by the broker,
    including the queue name and the specific reason (e.g. maximum length exceeded,
    queue unavailable). Available with RabbitMQ 4.3+.
    """

    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self) -> str:
        return repr(self.msg)
