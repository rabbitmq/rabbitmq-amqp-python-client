# Reliable messaging, containerized

[`Dockerfile`](Dockerfile) packages [`reliable.py`](../../reliable.py) — a
long-running publish/consume demo that keeps running through a broker
disconnect and reconnect — as a container image, following the same pattern
as [rabbitmq-amqp-go-client's own reliable
example](https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/reliable).
Every runtime parameter is external: nothing broker-specific is baked into
the image, and every environment variable `reliable.py` reads can be set at
`docker run` time instead.

## Build

Run from the repository root, since the build needs both the installable
package and the example script:

```sh
docker build -f docs/examples/docker/reliable/Dockerfile -t reliable-example .
```

## Run

```sh
docker run --rm \
  -e AMQP_HOST=rabbitmq \
  -e MESSAGES_TO_SEND=1000 \
  reliable-example
```

Against a broker on the Docker host rather than another container, use
`--network host` (Linux) or `-e AMQP_HOST=host.docker.internal` (macOS/Windows).

The image defaults `IS_SILENT=true`, since a container has no interactive
stdin to press a key on: the process keeps publishing, consuming and logging
periodic stats until the container is stopped (`docker stop`), rather than
waiting for input like an un-containerized run does by default.

## Environment variables

Each variable sets that setting's value directly — there are no command-line
flags, matching the Go example this is modeled on. Defaults below are the
image's own (set in the `Dockerfile`), which match `reliable.py`'s
un-containerized defaults except where noted.

| Variable | Default | Purpose |
|---|---|---|
| `QUEUE_NAME` | `clients-integration` | Quorum queue to declare, publish to and consume from. |
| `MESSAGES_TO_SEND` | `500000` | How many messages the publish loop attempts; must be > 0. |
| `IS_SILENT` | `true` | Run forever instead of waiting on stdin before tearing down. |
| `DELAY_MESSAGE` | `false` | Pace the publish loop with a small sleep between messages. |
| `AMQP_HOST` | `localhost` | Broker host. |
| `AMQP_PORT` | unset (5672, or 5671 with `AMQP_TLS=true`) | Broker port. |
| `AMQP_USER` | `guest` | SASL PLAIN username. |
| `AMQP_PASSWORD` | `guest` | SASL PLAIN password. |
| `AMQP_VHOST` | `/` | Virtual host. |
| `AMQP_TLS` | `false` | Wrap the connection in TLS. |

Boolean variables accept `true`/`false`, `yes`/`no`, `on`/`off` or `1`/`0`,
case-insensitively; any other value is rejected before the container connects
to anything.
