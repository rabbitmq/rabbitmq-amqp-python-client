# type: ignore


from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    Connection,
    Converter,
    Environment,
    Event,
    Message,
    OutcomeState,
    QuorumQueueSpecification,
)

MESSAGES_TO_PUBLISH = 200


# create a responder

class Responder:
    class ResponderMessageHandler(AMQPMessagingHandler):

        def __init__(self):
            super().__init__()
            self._publisher = None

        def set_publisher(self, publisher):
            self._publisher = publisher

        def on_amqp_message(self, event: Event):
            # process the message and create a response
            print("******************************************************")
            print("received message: {} ".format(Converter.bytes_to_string(event.message.body)))
            response_body = Converter.bytes_to_string(
                event.message.body) + "-from the server"
            response_message = Message(
                body=Converter.string_to_bytes(response_body))
            # publish response to the reply_to address with the same correlation_id
            response_message.correlation_id = event.message.correlation_id
            response_message.address = event.message.reply_to
            print("sending back: {} ".format(response_body))
            status = self._publisher.publish(
                message=response_message
            )
            if status.remote_state == OutcomeState.ACCEPTED:
                print("message accepted to {}".format(response_message.address))
            elif status.remote_state == OutcomeState.RELEASED:
                print("message not routed")
            elif status.remote_state == OutcomeState.REJECTED:
                print("message not rejected")

            self.delivery_context.accept(event)
            print("------------------------------------------------------")

    def __init__(self, request_queue_name: str, environment: Environment):
        self.request_queue_name = request_queue_name
        self.connection = None
        self.consumer = None
        self.publisher = None
        self._environment = environment

    def start(self):
        self.connection = self._environment.connection()
        self.connection.dial()
        self.connection.management().delete_queue(self.request_queue_name)
        self.connection.management().declare_queue(
            queue_specification=QuorumQueueSpecification(self.request_queue_name))
        self.publisher = self.connection.publisher()
        handler = self.ResponderMessageHandler()
        handler.set_publisher(self.publisher)

        self.consumer = self.connection.consumer(destination=AddressHelper.queue_address(self.request_queue_name),
                                                 message_handler=handler
                                                 )
        addr = self.consumer.address
        print("Responder listening on address: {}".format(addr))
        try:
            self.consumer.run()
        except KeyboardInterrupt:
            print("Responder stopping...")


def create_connection(environment: Environment) -> Connection:
    connection = environment.connection()
    connection.dial()
    return connection


def main() -> None:
    print("connection_consumer to amqp server")
    environment = Environment(uri="amqp://guest:guest@localhost:5672/")
    responder = Responder(request_queue_name="rpc_queue", environment=environment)
    responder.start()


if __name__ == "__main__":
    main()
