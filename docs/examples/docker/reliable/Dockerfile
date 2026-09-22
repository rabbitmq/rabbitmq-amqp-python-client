# Runs docs/examples/reliable.py as a container. Every runtime parameter is
# external — set with `docker run -e` — following the same pattern as
# rabbitmq-amqp-go-client's docs/examples/reliable/Dockerfile, which this
# example is itself modeled on.
#
# Build context must be the repository root, since the image needs both the
# installable package and the example script:
#
#     docker build -f docs/examples/docker/reliable/Dockerfile -t reliable-example .
#
# Run against a broker reachable as `rabbitmq`, publishing 1000 messages
# non-interactively:
#
#     docker run --rm -e AMQP_HOST=rabbitmq -e MESSAGES_TO_SEND=1000 reliable-example
#
# See docs/examples/docker/reliable/README.md for the full variable table.

FROM python:3.12-slim

WORKDIR /app

# The client (pyproject.toml's `dependencies = []`) is pure Python with no
# third-party dependencies, so it needs no `pip install` step — copying the
# package onto PYTHONPATH is enough, and keeps the build itself needing no
# network access beyond pulling this base image.
COPY rabbitmq_amqp_python_client ./rabbitmq_amqp_python_client
COPY docs/examples/reliable.py ./reliable.py
ENV PYTHONPATH=/app

# Matches reliable.py's own defaults, except QUEUE_NAME and IS_SILENT: a
# container has no interactive stdin to press a key on, and
# "clients-integration" is the queue name shared across the RabbitMQ clients'
# own Dockerfiles. AMQP_PASSWORD's default is RabbitMQ's own documented
# local-broker default, not a real secret.
ENV AMQP_HOST=localhost \
    AMQP_USER=guest \
    AMQP_PASSWORD=guest \
    AMQP_VHOST=/ \
    AMQP_TLS=false \
    QUEUE_NAME=clients-integration \
    MESSAGES_TO_SEND=500000 \
    IS_SILENT=true \
    DELAY_MESSAGE=false

ENTRYPOINT ["python", "reliable.py"]
