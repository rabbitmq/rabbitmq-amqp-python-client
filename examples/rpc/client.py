import threading
import time
from typing import Optional

from rabbitmq_amqp_python_client import (
    AddressHelper,
    AMQPMessagingHandler,
    ConsumerOptions,
    ConsumerSettleStrategy,
    Converter,
    Environment,
    Event,
    Message,
)


class _ReplyWaiter(AMQPMessagingHandler):
    """Collects a single reply delivered to the direct reply-to consumer."""

    def __init__(self) -> None:
        super().__init__()
        self._event = threading.Event()
        self._reply: Optional[Message] = None

    def on_amqp_message(self, event: Event) -> None:
        self._reply = event.message
        self._event.set()

    def begin_wait(self) -> None:
        self._event.clear()
        self._reply = None

    def wait_reply(self, timeout: float = 60.0) -> Message:
        if not self._event.wait(timeout):
            raise TimeoutError("Timed out waiting for RPC reply")
        if self._reply is None:
            raise RuntimeError("Reply was not captured")
        return self._reply


class Requester:
    def __init__(self, request_queue_name: str, environment: Environment):
        self._reply_handler = _ReplyWaiter()
        self.connection = environment.connection()
        self.connection.dial()
        self.publisher = self.connection.publisher(
            AddressHelper.queue_address(request_queue_name)
        )
        # connection is not thread safe.
        # You need another connection to run the consumer in a different thread.
        self.consumer_connection = environment.connection()
        self.consumer_connection.dial()

        self.consumer = self.consumer_connection.consumer(
            consumer_options=ConsumerOptions(
                settle_strategy=ConsumerSettleStrategy.DirectReplyTo
            ),
            message_handler=self._reply_handler,
        )
        self._run_thread = threading.Thread(target=self.consumer.run, daemon=True)
        self._run_thread.start()
        print("[Requester] Connected both publisher and consumer")
        print("[Requester] Consumer reply address is {}".format(self.consumer.address))

    def send_request(self, request_body: str, correlation_id: str) -> Message:
        self._reply_handler.begin_wait()
        message = Message(body=Converter.string_to_bytes(request_body))
        message.reply_to = self.consumer.address
        message.correlation_id = correlation_id
        self.publisher.publish(message=message)
        r = self._reply_handler.wait_reply()
        return r


def main() -> None:
    print("Connecting to AMQP server")
    environment = Environment(uri="amqp://guest:guest@localhost:5672/")
    requester = Requester(request_queue_name="rpc_queue", environment=environment)
    time.sleep(1)
    for i in range(500):
        correlation_id = str(i)
        request_body = "hello {}".format(i)
        print("******************************************************")
        print("[Requester] Sending request: {}".format(request_body))
        response_message = requester.send_request(
            request_body=request_body, correlation_id=correlation_id
        )
        response_body = Converter.bytes_to_string(response_message.body)
        print(
            "[Requester] Received response: {} - correlation_id: {}".format(
                response_body, response_message.correlation_id
            )
        )
        print("------------------------------------------------------")
        time.sleep(0.1)


if __name__ == "__main__":
    main()
