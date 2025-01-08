from .entities import BindingSpecification


def is_unreserved(char: str) -> bool:
    # According to RFC 3986, unreserved characters are A-Z, a-z, 0-9, '-', '.', '_', and '~'
    return char.isalnum() or char in "-._~"


def encode_path_segment(input_string: str) -> str:
    encoded = []

    # Iterate over each character in the input string
    for char in input_string:
        # Check if the character is an unreserved character
        if is_unreserved(char):
            encoded.append(char)  # Append as is
        else:
            # Encode character to %HH format
            encoded.append(f"%{ord(char):02X}")

    return "".join(encoded)


def exchange_address(exchange_name: str, routing_key: str = "") -> str:
    if routing_key == "":
        path = "/exchanges/" + encode_path_segment(exchange_name)
    else:
        path = (
            "/exchanges/"
            + encode_path_segment(exchange_name)
            + "/"
            + encode_path_segment(routing_key)
        )

    return path


def queue_address(queue_name: str) -> str:
    path = "/queues/" + encode_path_segment(queue_name)

    return path


def purge_queue_address(queue_name: str) -> str:
    path = "/queues/" + encode_path_segment(queue_name) + "/messages"

    return path


def path_address() -> str:
    path = "/bindings"

    return path


def binding_path_with_exchange_queue(bind_specification: BindingSpecification) -> str:
    binding_path_wth_exchange_queue_key = (
        "/bindings"
        + "/"
        + "src="
        + encode_path_segment(bind_specification.source_exchange)
        + ";"
        + "dstq="
        + encode_path_segment(bind_specification.destination_queue)
        + ";key="
        + encode_path_segment(bind_specification.binding_key)
        + ";args="
    )
    return binding_path_wth_exchange_queue_key
