from rabbitmq_amqp_python_client import (
    BindingSpecification,
    Connection,
    ExchangeSpecification,
    Message,
    QueueSpecification,
    QueueType,
    exchange_address,
)


def main() -> None:
    exchange_name = "test-exchange"
    queue_name = "example-queue"
    routing_key = "routing-key"
    connection = Connection("amqp://guest:guest@localhost:5672/")

    print("connection to amqp server")
    connection.dial()

    management = connection.management()

    print("declaring exchange and queue")
    management.declare_exchange(ExchangeSpecification(name=exchange_name, arguments={}))

    management.declare_queue(
        QueueSpecification(name=queue_name, queue_type=QueueType.quorum)
    )

    print("binding queue to exchange")
    bind_name = management.bind(
        BindingSpecification(
            source_exchange=exchange_name,
            destination_queue=queue_name,
            binding_key=routing_key,
        )
    )

    addr = exchange_address(exchange_name, routing_key)

    print("create a publisher and publish a test message")
    publisher = connection.publisher(addr)

    publisher.publish(Message(body="test"))

    publisher.close()

    print("unbind")
    management.unbind(bind_name)

    print("purging the queue")
    management.purge_queue(queue_name)

    print("delete queue")
    management.delete_queue(queue_name)

    print("delete exchange")
    management.delete_exchange(exchange_name)

    print("closing connections")
    management.close()
    connection.close()


if __name__ == "__main__":
    main()
