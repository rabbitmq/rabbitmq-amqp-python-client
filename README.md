# RabbitMQ AMQP 1.0 Python Client

This library is in early stages of development. It is meant to be used with RabbitMQ 4.0.

## How to Build the project and run the tests

- Start a RabbitMQ 4.x broker
- poetry build: build the source project
- poetry install: resolves and install dependencies
- poetry run pytest: run the tests

## Getting Started

An example is provide in ./getting_started_main.py you can run it after starting a RabbitMQ 4.0 broker with:

poetry run python ./examples/getting_started/main.py


