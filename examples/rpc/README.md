RPC example
===

This example demonstrates how to set up a simple RPC (Remote Procedure Call) server and client using [Direct reply to feature](https://www.rabbitmq.com/docs/direct-reply-to). 
The example is very basic the correlation id is set but not used to match responses to requests.

Setup
---

To run this example, you need to have RabbitMQ >=4.2 server running locally.
Then run the python scripts in separate terminal windows.
```bash
$ python3 server.py
connection_consumer to amqp server
Responder listening on address: /queues/rpc_queue
```

Then in another terminal window run:
```bash
$ python3 client.py
connection_consumer to amqp server
connected both publisher and consumer
consumer reply address is /queues/amq.rabbitmq.reply-to.g1h2AA5yZXBseUA2ODc4MTMzNAAAcEoAAAAAaS8eQg%3D%
```

The `rpc_queue` is the queue where the server listens for incoming RPC requests.</br>
The `amq.rabbitmq.reply-to.g1h2AA...` is a special direct-reply-to queue used by the client to receive responses.

Use standard queues for reply 
===

If you want to use standard queues for replies instead of the direct-reply-to feature.
