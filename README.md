# RabbitMQ AMQP 1.0 Python version 2.x

RabbitMQ AMQP 1.0 Python client version 2.x for RabbitMQ 4.x.

This client version built from scratch. The primary reason for this rewrite is to remove the dependency on the `qpid-proton` C library present in version 1, providing a fully native Python client. 
This client is inspired by the Java and .NET AMQP 1.0 clients, aiming to deliver the same behavior and user experience across ecosystems.

The client is different from 1.0 and not compatible, so read the documentation before updating the client.


## Installation

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

> This repository's own sandbox could not reach PyPI, so its `.venv` was created
> with `python3 -m venv --system-site-packages .venv` to pick up an already
> installed ruff/mypy/pytest, and the examples are run with `PYTHONPATH=.`
> (the project root) instead of an editable install. That is an environment
> quirk, not a project requirement — with the editable install above,
> `import rabbitmq_amqp_python_client` just works from anywhere.

## Quick start

```python
from src import Connection, ConnectionParameters, Context, Message

connection = Connection(ConnectionParameters())  # localhost:5672, guest/guest
try:
    connection.management().queue("my-queue").declare()

    def handle(context: Context, message: Message) -> None:
        print("received", message.body_as_string())
        context.accept()

    consumer = connection.consumer_builder().queue("my-queue").message_handler(handle).build()
    publisher = connection.publisher_builder().queue("my-queue").build()

    result = publisher.publish(Message("hello"))
    print("published:", result.outcome.state.value)  # -> accepted

    publisher.close()
    consumer.close()
    connection.management().queue("my-queue").delete()
finally:
    connection.close()
```

## Examples

[`docs/examples/README.md`](docs/examples/README.md) catalogues twelve runnable
scripts, in reading order — from `basic_connection.py` through publishing and
consuming to auto-reconnection, presettled consumers, direct reply-to,
rejection reasons, single-active-consumer notifications and stream filtering.
Each one talks to a local broker:

```sh
PYTHONPATH=. .venv/bin/python docs/examples/basic_connection.py
```

## Development

```sh
make install          # create .venv and install the package with its dev extras
make format           # ruff format, then ruff check --fix
make lint             # ruff check and ruff format --check
make typecheck        # mypy src, in strict mode
make test-unit        # the unit suite; no broker needed
make test-integration # the integration suite; needs a broker on localhost:5672
make test             # both suites
```

The example scripts are type-checked too:

```sh
MYPYPATH=. .venv/bin/python -m mypy docs/examples/*.py
```

## Project layout

`src/` holds the client: `wire/` is the protocol
layer (type codec, frames, performatives, messages, SASL) and knows nothing
above itself, and `connection.py`, `session.py`, `link.py`, `management.py`,
`publisher.py`, `consumer.py` and `reconnection.py` build the public API on top
of it. `tests/unit` covers each of those against an in-process fake broker and
needs no network; `tests/integration` runs the same surface against a real
broker and is marked `integration`. `docs/examples/` holds the runnable scripts
the specifications ask for.
