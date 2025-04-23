class Converter:

    @staticmethod
    def bytes_to_string(body: bytes) -> str:
        """
        Convert the body of a message to a string.

        Args:
            body: The body of the message

        Returns:
            str: The string representation of the body
        """
        return "".join(map(chr, body))

    @staticmethod
    def string_to_bytes(body: str) -> bytes:
        """
        Convert a string to the body of a message.

        Args:
            body: The string to convert

        Returns:
            bytes: The byte representation of the string
        """
        return str.encode(body)
