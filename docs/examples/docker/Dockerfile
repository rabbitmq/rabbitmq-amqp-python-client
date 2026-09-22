# Runs docs/examples/console_application.py as a container. Every runtime
# parameter is external — set with `docker run -e` — following the same
# pattern as rabbitmq-amqp-go-client's docs/examples/reliable/Dockerfile.
#
# Build context must be the repository root, since the image needs both the
# installable package and the example script:
#
#     docker build -f docs/examples/docker/Dockerfile -t console-application .
#
# Run against a broker reachable as `rabbitmq`, publishing 500 messages:
#
#     docker run --rm -e AMQP_HOST=rabbitmq -e AMQP_MESSAGES=500 console-application
#
# See docs/examples/docker/README.md for the full AMQP_* variable table.

FROM python:3.12-slim

WORKDIR /app

# The client (pyproject.toml's `dependencies = []`) is pure Python with no
# third-party dependencies, so it needs no `pip install` step — copying the
# package onto PYTHONPATH is enough, and keeps the build itself needing no
# network access beyond pulling this base image.
COPY rabbitmq_amqp_python_client ./rabbitmq_amqp_python_client
COPY docs/examples/console_application.py ./console_application.py
ENV PYTHONPATH=/app

# Matches Options()'s own defaults; AMQP_PORT and AMQP_QUEUE are left unset so
# the script's own None/generated-name defaults apply unless overridden.
# AMQP_PASSWORD's default is RabbitMQ's own documented local-broker default,
# not a real secret.
ENV AMQP_HOST=localhost \
    AMQP_USER=guest \
    AMQP_PASSWORD=guest \
    AMQP_VHOST=/ \
    AMQP_MESSAGES=1000000 \
    AMQP_QUEUE_TYPE=classic \
    AMQP_KEEP_QUEUE=false \
    AMQP_CONSUME_TIMEOUT=30 \
    AMQP_PUBLISH_TIMEOUT=5 \
    AMQP_STATS_INTERVAL=1 \
    AMQP_TLS=false \
    AMQP_RECOVERY=true \
    AMQP_RECOVERY_TOPOLOGY=false \
    AMQP_QUEUE="clients-integration"

ENTRYPOINT ["python", "console_application.py"]
