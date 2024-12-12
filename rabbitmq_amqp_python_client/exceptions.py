class ValidationCodeException(Exception):
    # Constructor or Initializer
    def __init__(self, msg: str):
        self.msg = msg

    # __str__ is to print() the value
    def __str__(self) -> str:
        return repr(self.msg)
