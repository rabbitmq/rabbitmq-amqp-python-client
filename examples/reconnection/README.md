Automatic reconnection
===

You can use this example to test automatic reconnection.

`reconnection_example.py` starts a publisher and a consumer that run **independently**:
each one lives in its own thread and uses its own connection (the TCP connection is not thread safe),
so the consumer keeps consuming while the publisher is still publishing.

The publisher sends about 100 messages per second until you press Ctrl+C.
Every 5 seconds the main thread prints a status line, for example:

```
14:22:37.883 [status   ] published=241 (+241) consumed=241 (+241) in-flight=0 publish-errors=0
```

Each log line has a timestamp and a source tag (`main`, `publisher`, `consumer`, `status`).
When the connection is lost you will see `connection lost, waiting for recovery...` from the
publisher and/or the consumer, and `connection recovered` once the publisher can publish again.

From the RabbitMQ UI you can break a connection to see the automatic reconnection happening.

Same for Consumers and Managements handlers.

In case of streams the connection will restart consuming from the last consumed offset.

You can control some reconnection parameters with the RecoveryConfiguration dataclass that you can pass to the Connection class, where you can specify 
the backoff interval and the maximum_retries before the client gives up to reconnect.

To disable automatic reconnection you can set active_recovery of RecoveryConfiguration to False

Press Ctrl+C to stop: the publisher stops, the consumer is stopped, and the binding, queue and exchange are deleted.
The tunables `PUBLISH_INTERVAL_SECONDS` and `STATUS_INTERVAL_SECONDS` are at the top of the example.
