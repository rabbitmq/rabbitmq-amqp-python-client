import time

from rabbitmq_amqp_python_client import (
    AddressHelper,
    ConsumerFeature,
    ConsumerOptions,
    Converter,
    Environment,
    Message,
)


class Requester:
    def __init__(self, request_queue_name: str, environment: Environment):
        self.connection = environment.connection()
        self.connection.dial()
        self.publisher = self.connection.publisher(
            AddressHelper.queue_address(request_queue_name)
        )
        self.consumer = self.connection.consumer(
            consumer_options=ConsumerOptions(feature=ConsumerFeature.DirectReplyTo)
        )
        print("connected both publisher and consumer")
        print("consumer reply address is {}".format(self.consumer.address))

    def send_request(self, request_body: str, correlation_id: str) -> Message:
        message = Message(body=Converter.string_to_bytes(request_body))
        message.reply_to = self.consumer.address
        message.correlation_id = correlation_id
        self.publisher.publish(message=message)
        return self.consumer.consume()


def main() -> None:
    print("Connecting to AMQP server")
    environment = Environment(uri="amqp://guest:guest@localhost:5672/")
    requester = Requester(request_queue_name="rpc_queue", environment=environment)
    for i in range(10):
        correlation_id = str(i)
        request_body = "hello {}".format(i)
        print("******************************************************")
        print("Sending request: {}".format(request_body))
        response_message = requester.send_request(
            request_body=request_body, correlation_id=correlation_id
        )
        response_body = Converter.bytes_to_string(response_message.body)
        print(
            "Received response: {} - correlation_id: {}".format(
                response_body, response_message.correlation_id
            )
        )
        print("------------------------------------------------------")
        time.sleep(1)


if __name__ == "__main__":
    main()
