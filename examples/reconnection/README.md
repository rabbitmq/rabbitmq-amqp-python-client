Automatic reconnection
===

You can use this example to test automatic reconnection.

The scenario is publishing and consuming a lot of messages in a queue.

From the RabbitMQ UI you can break a connection to see the automatic reconnection happening.

Same for Consumers.

In case of streams the connection will restart consuming from the last consumed offset.

You can control some reconnection parameters with the RecoveryConfiguration dataclass, where you can specify 
the backoff interval and the maximum_retries before the client gives up.

To disable automatic reconnection you can set active_recovery of RecoveryConfiguration to False

