from typing import Optional

from rabbitmq_amqp_python_client import (
    BindingSpecification,
    Connection,
    ExchangeSpecification,
    ExchangeType,
    Management,
    Message,
    QuorumQueueSpecification,
)


def publish_messages(
    connection: Connection,
    messages_to_send: int,
    queue_name,
    filters: Optional[list[str]] = None,
) -> None:
    annotations = {}
    if filters is not None:
        for filter in filters:
            annotations = {"x-stream-filter-value": filter}

    publisher = connection.publisher("/queues/" + queue_name)
    # publish messages_to_send messages
    for i in range(messages_to_send):
        publisher.publish(Message(body="test" + str(i), annotations=annotations))
    publisher.close()


def setup_dead_lettering(management: Management) -> str:

    exchange_dead_lettering = "exchange-dead-letter"
    queue_dead_lettering = "queue-dead-letter"
    binding_key = "key_dead_letter"

    # configuring dead lettering
    management.declare_exchange(
        ExchangeSpecification(
            name=exchange_dead_lettering,
            exchange_type=ExchangeType.fanout,
            arguments={},
        )
    )
    management.declare_queue(QuorumQueueSpecification(name=queue_dead_lettering))
    bind_path = management.bind(
        BindingSpecification(
            source_exchange=exchange_dead_lettering,
            destination_queue=queue_dead_lettering,
            binding_key=binding_key,
        )
    )

    return bind_path


def cleanup_dead_lettering(management: Management, bind_path: str) -> None:

    exchange_dead_lettering = "exchange-dead-letter"
    queue_dead_lettering = "queue-dead-letter"

    management.unbind(bind_path)
    management.delete_exchange(exchange_dead_lettering)
    management.delete_queue(queue_dead_lettering)
