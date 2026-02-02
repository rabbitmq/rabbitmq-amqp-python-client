# FIFO Consumer with Pre-Settled Deliveries

This example demonstrates how to use `FIFOConsumerOptions` with the `pre_settled` parameter to enable at-most-once delivery semantics for FIFO (Classic and Quorum) queues.

## Pre-Settled Deliveries

When `pre_settled=True`:
- Messages are automatically settled when received
- Messages cannot be redelivered if processing fails
- Provides **at-most-once** delivery semantics
- Suitable for use cases where message loss is acceptable (e.g., metrics, logs, sensor data)

When `pre_settled=False` (default):
- Messages require explicit acknowledgment
- Messages can be redelivered if not acknowledged
- Provides **at-least-once** delivery semantics
- Suitable for use cases where message loss is not acceptable

## Running the Example

```bash
python examples/pre_settled_consumer/example_pre_settled_consumer.py
```

Make sure RabbitMQ is running on `localhost:5672` with default credentials (`guest`/`guest`).

## Code Example

```python
from rabbitmq_amqp_python_client import (
    AddressHelper,
    Connection,
    Environment,
    ConsumerOptions,
    QuorumQueueSpecification,
)

# Create connection
environment = Environment(uri="amqp://guest:guest@localhost:5672/")
connection = environment.connection()
connection.dial()

# Declare queue
management = connection.management()
management.declare_queue(QuorumQueueSpecification(name="my-queue"))

# Create consumer with pre_settled=True
addr_queue = AddressHelper.queue_address("my-queue")
consumer = connection.consumer(
    addr_queue,
    message_handler=MyMessageHandler(),
    consumer_options=ConsumerOptions(pre_settled=True),
)

# Start consuming
consumer.run()
```

## Notes

- `FIFOConsumerOptions` is only valid for Classic and Quorum queues, not Stream queues
- Pre-settled deliveries cannot be redelivered, so ensure your application can handle message loss
- Use pre-settled deliveries for non-critical data where performance is more important than reliability
