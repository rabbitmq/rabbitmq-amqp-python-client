## RabbitMQ AMQP 1.0 Python Client
This library is meant to be used with RabbitMQ 4.0. Suitable for testing in pre-production environments.

The client is distributed via [`PIP`](https://pypi.org/project/rabbitmq-amqp-python-client/):
```bash
 pip install rabbitmq-amqp-python-client
```

### Getting Started    

Inside the [examples](./examples) folder you can find a set of examples that show how to use the client.


### Documentation

[Client Guide](https://www.rabbitmq.com/client-libraries/amqp-client-libraries) select the python section.


### Build

- Start a RabbitMQ 4.x broker
- poetry build: build the source project
- poetry install: resolves and install dependencies
- poetry run pytest: run the tests





