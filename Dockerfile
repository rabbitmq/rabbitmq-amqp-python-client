FROM rabbitmq:4-management
#FROM pivotalrabbitmq/rabbitmq:sha-ae9fbb7bd5982aff099293adbb1edcd616ef806f


COPY .ci/conf/rabbitmq.conf /etc/rabbitmq/rabbitmq.conf
COPY .ci/conf/enabled_plugins /etc/rabbitmq/enabled_plugins

COPY .ci/certs /etc/rabbitmq/certs
