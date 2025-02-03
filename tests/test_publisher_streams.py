from rabbitmq_amqp_python_client import (
    StreamSpecification,
    QueueType,
    Management,
    AddressHelper,
    StreamFilterOptions,
    Connection,
)

from .utils import publish_messages
from .conftest import MyMessageHandlerAccept, ConsumerTestException


def test_queue_info_for_stream_with_validations(connection: Connection) -> None:

    stream_name = "test_stream_info_with_validation"
    messages_to_send = 200

    queue_specification = StreamSpecification(
        name=stream_name,
    )
    management = connection.management()
    management.declare_queue(queue_specification)

    publish_messages(connection, messages_to_send, stream_name)

    addr_queue = AddressHelper.queue_address(stream_name)

    stream_filter_options = StreamFilterOptions()
    stream_filter_options.offset(0)

    consumer = connection.consumer(addr_queue,  handler=MyMessageHandlerAccept())

    try:
        print("running")
        consumer.run()
    # ack to terminate the consumer
    except ConsumerTestException:
        pass

    consumer.close()

    management.delete_queue(stream_name)

    #assert stream_info.name == stream_name
    #assert stream_info.queue_type == QueueType.stream
    #assert stream_info.message_count == 0