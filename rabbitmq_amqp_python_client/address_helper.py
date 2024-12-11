from .entities import BindingSpecification


def exchange_address(name: str) -> str:
    path = "/exchanges/" + name

    return path


def queue_address(name: str) -> str:
    path = "/queues/" + name

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
