Automatic reconnection
===

You can use this example to test automatic reconnection.

The scenario is publishing and consuming a lot of messages in a queue.

From the RabbitMQ UI you can break a connection to see the automatic reconnection happening.

Same for Consumers.

In case of streams we connection will restart consuming from the last consumed offset.

