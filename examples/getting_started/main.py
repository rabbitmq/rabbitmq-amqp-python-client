from rabbitmq_amqp_python_client import (
    BindingSpecification,
    Connection,
    ExchangeSpecification,
    QueueSpecification,
    QueueType,
)


def main() -> None:
    exchange_name = "test-exchange"
    queue_name = "example-queue"
    routing_key = "routing-key"
    connection = Connection("amqp://guest:guest@localhost:5672/")

    connection.dial()

    management = connection.management()

    management.declare_exchange(ExchangeSpecification(name=exchange_name, arguments={}))

    management.declare_queue(
        QueueSpecification(name=queue_name, queue_type=QueueType.quorum, arguments={})
    )

    binding_exchange_queue_path = management.bind(
        BindingSpecification(
            source_exchange=exchange_name,
            destination_queue=queue_name,
            binding_key=routing_key,
        )
    )

    """
    addr = exchange_address(exchange_name, routing_key)
    """

    """
    publisher = connection.publisher(addr, "getting-started-publisher")
    """

    """
    message = Message(
        body='test',
        address='/queues/getting-started-exchangemessage',
    )

    publisher.Publish(message)
    publisher.close()
    """

    management.unbind(binding_exchange_queue_path)

    """
    management.purge_queue(queue_info.name)
    """

    # management.delete_queue(queue_name)

    # management.delete_exchange(exchange_name)

    management.close()

    connection.close()


if __name__ == "__main__":
    main()
