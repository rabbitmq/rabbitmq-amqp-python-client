def exchange_address(name: str) -> str:
    path = "/exchanges/" + name

    return path


def queue_address(name: str) -> str:
    path = "/queues/" + name

    return path
