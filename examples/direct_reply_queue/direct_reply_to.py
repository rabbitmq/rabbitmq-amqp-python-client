# type: ignore


from rabbitmq_amqp_python_client import (  # PosixSSlConfigurationContext,; PosixClientCert,
    AMQPMessagingHandler,
    Connection,
    Converter,
    DirectReplyToConsumerOptions,
    Environment,
    Event,
    Message,
    OutcomeState,
)

MESSAGES_TO_PUBLISH = 200


class MyMessageHandler(AMQPMessagingHandler):

    def __init__(self):
        super().__init__()
        self._count = 0

    def on_amqp_message(self, event: Event):
        print(
            "received message: {} ".format(
                Converter.bytes_to_string(event.message.body)
            )
        )

        # accepting
        self.delivery_context.accept(event)

        self._count = self._count + 1
        print("count " + str(self._count))

        if self._count == MESSAGES_TO_PUBLISH:
            print("received all messages")

    def on_connection_closed(self, event: Event):
        # if you want you can add cleanup operations here
        print("connection closed")

    def on_link_closed(self, event: Event) -> None:
        # if you want you can add cleanup operations here
        print("link closed")


def create_connection(environment: Environment) -> Connection:
    connection = environment.connection()
    connection.dial()
    return connection


def main() -> None:
    print("connection to amqp server")
    environment = Environment(uri="amqp://guest:guest@localhost:5672/")
    connection = create_connection(environment)
    consumer = connection.consumer(message_handler=MyMessageHandler(),
                                   consumer_options=DirectReplyToConsumerOptions())
    addr = consumer.get_queue_address()
    print("connecting to address: {}".format(addr))
    publisher = create_connection(environment).publisher(addr)

    for i in range(MESSAGES_TO_PUBLISH):
        msg = Message(
            body=Converter.string_to_bytes("test message {} ".format(i)))
        status = publisher.publish(msg)
        if status.remote_state == OutcomeState.ACCEPTED:
            print("message accepted")
        elif status.remote_state == OutcomeState.RELEASED:
            print("message not routed")
        elif status.remote_state == OutcomeState.REJECTED:
            print("message not rejected")

    try:
        consumer.run()
    except KeyboardInterrupt:
        pass

    consumer.close()

    connection.close()
    print("after connection closing")


if __name__ == "__main__":
    main()
