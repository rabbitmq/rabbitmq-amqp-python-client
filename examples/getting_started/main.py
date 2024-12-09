# from proton import Message

from rabbitmq_amqp_python_client import (
    Connection,
    ExchangeSpecification,
    QueueSpecification,
    QueueType,
)


def main():
    exchange_name = "example-exchange"
    queue_name = "example-queue"
    connection = Connection("amqp://guest:guest@localhost:5672/")

    connection.dial()

    management = connection.management()

    exchange_info = management.declare_exchange(
        ExchangeSpecification(name=exchange_name, arguments={})
    )

    queue_info = management.declare_queue(
        QueueSpecification(name=queue_name, queue_type=QueueType.quorum, arguments={})
    )

    """
    #management.bind(BindingSpecification{
		source_exchange:   exchange_name,
		destination_queue: queue_name,
		binding_key:       routing_key,
	})
    """

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

    """
    management.unbind(binding_path)
    """

    """
    management.purge_queue(queue_info.name)
    """

    """
    management.delete_queue(queue_info.name)
    """

    """
    management.delete_exchange(exchange_info.name)
    """

    management.close()

    connection.close()


if __name__ == "__main__":
    main()
