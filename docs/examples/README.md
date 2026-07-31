# Examples

Every example is a standalone script that talks to a local broker on
`localhost:5672` with the default `guest`/`guest` credentials. Run one from the
project root, with the project root itself on the path:

```sh
PYTHONPATH=. .venv/bin/python docs/examples/<example>.py
```

The table is in reading order: the first four are the client's core surface,
one layer at a time, and the rest build on them.

| Example | What it shows |
|---|---|
| [`basic_connection.py`](basic_connection.py) | The smallest possible program: `Connection(ConnectionParameters())` dials, authenticates and opens in one step, `state` reports `OPEN`, and `close()` shuts it down. Start here. |
| [`management_example.py`](management_example.py) | Topology over AMQP 1.0 and nothing else: declare a classic queue and a topic exchange, bind them with a routing key, list the bindings the broker reports, then unbind and delete all three. Every call is one request/response pair over the `/management` link pair. |
| [`publisher_example.py`](publisher_example.py) | The three targets a publisher can have — a queue, an exchange plus a routing key, and none at all. The anonymous publisher sends to two different addresses over one link by setting `properties.to` per message, and a publish to an unbound routing key shows a `RELEASED` outcome, which is not an error. |
| [`consumer_example.py`](consumer_example.py) | A `message_handler` that accepts and counts every delivery, `unsettled_message_count` while it does, and `pause()`/`unpause()` as flow control: messages published while paused stay queued and arrive the moment credit is restored. |
| [`auto_reconnection.py`](auto_reconnection.py) | A connection surviving a forced socket drop: publishers and consumers built before the drop keep working afterwards, and `RecoveryConfiguration(topology=True)` re-declares the topology the broker lost. |
| [`console_application.py`](console_application.py) | The whole client end to end — declare, publish, consume, report — as a scriptable smoke test with meaningful exit codes. `--help` lists its options. |
| [`performance_test.py`](performance_test.py) | A throughput/latency generator: many publishers and consumers on one or more connections, with periodic rate and latency reporting. |
| [`presettled_consumer_example.py`](presettled_consumer_example.py) | At-most-once consumption: `presettled()` attaches with `snd-settle-mode = settled`, so the broker settles every delivery itself, the handler never touches its `Context`, and `unsettled_message_count` stays at `0` for the consumer's whole life — including across a forced disconnect that auto-reconnection recovers from. |
| [`rejection_reason_example.py`](rejection_reason_example.py) | Why a publish was refused: a quorum queue with `max_length(5)` and the `reject-publish` overflow strategy returns a `REJECTED` outcome carrying `RejectionDetails.reason` and `.rejected_by_queue` (needs RabbitMQ 4.3+; the script says so when the broker supplies neither). |
| [`quorum_single_active_consumer.py`](quorum_single_active_consumer.py) | Single active consumer notifications on a quorum queue (needs RabbitMQ 4.3+): two consumers on a queue declared with `single_active_consumer(True)` each register a `single_active_consumer_state_changed` handler, and the broker tells one of them it is active and the other that it is standby. Only the active one receives messages; closing it promotes the standby one, which then starts receiving. |
| [`stream_filtering.py`](stream_filtering.py) | Reading a stream queue from a chosen offset and narrowing what arrives (needs RabbitMQ 4.2+ for the SQL filter): the same batch of messages is read back three times from `offset(FIRST)` — once with `filter().subject(...).property("region", ...)`, once with the equivalent `filter().sql(...)`, and once with the cheap, probabilistic `filter_values(...)` bloom filter, whose publisher side is just an `x-stream-filter-value` message annotation. The script reports whether the broker really enforces the SQL filter. |
