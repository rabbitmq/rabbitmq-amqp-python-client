# RabbitMQ AMQP 1.0 Python Client

This library is in early stages of development. It is meant to be used with RabbitMQ 4.0.

## How to Build the project and run the tests

- Start a RabbitMQ 4.x broker
- poetry build: build the source project
- poetry install: resolves and install dependencies
- poetry run pytest: run the tests

## Getting Started

An example is provided in ./getting_started_main.py you can run it after starting a RabbitMQ 4.0 broker with:

poetry run python ./examples/getting_started/main.py

### Creating a connection

A connection to the RabbitMQ AMQP 1.0 server can be established using the Connection object.

For example:

```python
    connection = Connection("amqp://guest:guest@localhost:5672/")
    connection.dial()
```

### Managing resources

Once we have a Connection object we can get a Management object in order to submit to the server management operations
(es: declare/delete queues and exchanges, purging queues, binding/unbinding objects ecc...)

For example (this code is declaring an exchange and a queue:

```python
    management = connection.management()

    print("declaring exchange and queue")
    management.declare_exchange(ExchangeSpecification(name=exchange_name, arguments={}))

    management.declare_queue(
        QuorumQueueSpecification(name=queue_name)
    )
```

### Publishing messages

Once we have a Connection object we can get a Publisher object in order to send messages to the server (to an exchange or queue)

For example:

```python
    addr_queue = AddressHelper.queue_address(queue_name)
    publisher = connection.publisher(addr)

    # publish messages
    for i in range(messages_to_publish):
        publisher.publish(Message(body="test"))

    publisher.close()
```

### Consuming messages

Once we have a Connection object we can get a Consumer object in order to consumer messages from the server (queue).

Messages are received through a callback

For example:

Create a class which extends AMQPMessagingHandler which defines at minimum the on_consumer method, that will receive the 
messages consumed:

```python
class MyMessageHandler(AMQPMessagingHandler):

    def __init__(self):
        super().__init__()
        self._count = 0

    def on_message(self, event: Event):
        print("received message: " + str(event.message.body))

        # accepting
        self.delivery_context.accept(event)
```

Then from connection get a consumer object:

```python
    addr_queue = AddressHelper.queue_address(queue_name)
    consumer = connection.consumer(addr_queue, handler=MyMessageHandler())

    try:
        consumer.run()
    except KeyboardInterrupt:
        pass

    consumer.close()
```

The consumer will run indefinitively waiting for messages to arrive.




