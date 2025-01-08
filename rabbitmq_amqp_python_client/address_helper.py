from .entities import BindingSpecification


def exchange_address(exchange_name: str, routing_key: str = "") -> str:
    if routing_key == "":
        path = "/exchanges/" + exchange_name
    else:
        path = "/exchanges/" + exchange_name + "/" + routing_key

    return path


def queue_address(name: str) -> str:
    path = "/queues/" + name

    return path


def purge_queue_address(name: str) -> str:
    path = "/queues/" + name + "/messages"

    return path


def path_address() -> str:
    path = "/bindings"

    return path


def binding_path_with_exchange_queue(bind_specification: BindingSpecification) -> str:
    binding_path_wth_exchange_queue_key = (
        "/bindings"
        + "/"
        + "src="
        + bind_specification.source_exchange
        + ";"
        + "dstq="
        + bind_specification.destination_queue
        + ";key="
        + bind_specification.binding_key
        + ";args="
    )
    return binding_path_wth_exchange_queue_key
