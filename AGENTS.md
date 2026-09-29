# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project

RabbitMQ AMQP 1.0 Python client (version 2.x), a native Python implementation
with no dependency on `qpid-proton`. See `README.md` for install instructions
and a quick-start example, and `README.md#project-layout` for how the code is
organized (`wire/` is the protocol layer; `connection.py`, `session.py`,
`link.py`, `management.py`, `publisher.py`, `consumer.py`, `reconnection.py`
build the public API on top of it).

## Setup

```sh
make install
```

## Common commands

```sh
make format           # ruff format, then ruff check --fix
make lint             # ruff check and ruff format --check
make typecheck        # mypy rabbitmq_amqp_python_client, in strict mode
make test-unit        # the unit suite; no broker needed
make test-integration # the integration suite; needs a broker on localhost:5672
make test             # both suites
```

Run `make lint` and `make typecheck` before considering a change done.
`make test-unit` needs no broker; `make test-integration` needs one running
on `localhost:5672`.

## Contributing

Every contribution — bug fix or feature — must include one of the following:

- A **failing test case** that reproduces the issue and passes once the fix
  is applied (add it under `tests/unit` or `tests/integration`, matching the
  existing style in that directory).
- A **small, runnable example** that clearly reproduces the issue, in the
  style of the scripts under `docs/examples/`.

Do not open a change without one of these — it's how reviewers (human or
agent) confirm the problem is real and that the fix actually addresses it.
