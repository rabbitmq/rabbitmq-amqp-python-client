Client examples
===
 - [Getting started](./getting_started/getting_started.py) - Producer and Consumer example without reconnection
 - [Reconnection](./reconnection/reconnection_example.py) - Producer and Consumer example with reconnection
 - [TLS](./tls/tls_example.py) - Producer and Consumer using a TLS connection
 - [Streams](./streams/example_with_streams.py) - Example supporting stream capabilities
 - [Oauth](./oauth/oAuth2.py) - Connection through Oauth token
 - [Streams with filters](./streams_with_filters/example_streams_with_filters.py) - Example supporting stream capabilities with filters
 - [Streams With Sql filters](./streams_with_sql_filters) - Example supporting stream capabilities with SQL filters
 - [Direct Reply To](./direct_reply_queue) - How to use [Direct Reply](https://www.rabbitmq.com/docs/direct-reply-to) feature
 - [Stream Consumer Offset Datetime](./stream_consumer_offset_datetime) - Example of stream consumer starting from a specific datetime
 - [RPC](./rpc) - Basic RPC example
 - [Rejected Messages](./rejected_messages/example_rejected_messages.py) - How to handle [rejected messages](https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release#amqp-rejection-reason) with `AmqpMessageRejectedException` (RabbitMQ 4.3+)
 - [Quorum Queue SAC Notification](./quorum_queue_sac/example_quorum_queue_sac.py) - Quorum Queue Single Active Consumer state-change notifications via `QuorumConsumerOptions.sac_state_handler` (RabbitMQ 4.3+)
