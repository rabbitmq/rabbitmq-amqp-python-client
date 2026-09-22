# Console application, containerized

[`Dockerfile`](Dockerfile) packages
[`console_application.py`](../console_application.py) — the client's
end-to-end publish/consume/report smoke test — as a container image. Every
runtime parameter is external: nothing broker-specific is baked into the
image, and every one of the flags `console_application.py --help` lists can
be set with an `AMQP_*` environment variable at `docker run` time instead of
a command-line flag.

## Build

Run from the repository root, since the build needs both the installable
package and the example script:

```sh
docker build -f docs/examples/docker/Dockerfile -t console-application .
```

## Run

```sh
docker run --rm \
  -e AMQP_HOST=rabbitmq \
  -e AMQP_MESSAGES=500 \
  -e AMQP_QUEUE_TYPE=quorum \
  console-application
```

Against a broker on the Docker host rather than another container, use
`--network host` (Linux) or `-e AMQP_HOST=host.docker.internal` (macOS/Windows).

## Environment variables

Each variable sets that flag's default; a command-line argument appended
after the image name still wins over both. Defaults below are the image's
own (set in the `Dockerfile`), which match `console_application.py`'s
un-containerized defaults except where noted.

| Variable | Flag | Default | Purpose |
|---|---|---|---|
| `AMQP_HOST` | `--host` | `localhost` | Broker host. |
| `AMQP_PORT` | `--port` | unset (5672, or 5671 with `AMQP_TLS=true`) | Broker port. |
| `AMQP_USER` | `--user` | `guest` | SASL PLAIN username. |
| `AMQP_PASSWORD` | `--password` | `guest` | SASL PLAIN password. |
| `AMQP_VHOST` | `--vhost` | `/` | Virtual host. |
| `AMQP_TLS` | `--tls` | `false` | Wrap the connection in TLS. |
| `AMQP_MESSAGES` | `--messages` | `1000000` | How many messages to publish; must be > 0. |
| `AMQP_QUEUE_TYPE` | `--queue-type` | `classic` | One of `classic`, `quorum`, `stream`. |
| `AMQP_QUEUE` | `--queue` | unset (a generated `console-app-*` name) | Queue name. |
| `AMQP_KEEP_QUEUE` | `--keep-queue` | `false` | Do not delete the queue during teardown. |
| `AMQP_CONSUME_TIMEOUT` | `--consume-timeout` | `30` | Seconds to wait for consumption to catch up. |
| `AMQP_PUBLISH_TIMEOUT` | `--publish-timeout` | `5` | Per-call publish timeout, in seconds. |
| `AMQP_STATS_INTERVAL` | `--stats-interval` | `1` | Seconds between periodic stats blocks. |
| `AMQP_RECOVERY` | `--recovery`/`--no-recovery` | `true` | Auto-reconnect after an unexpected disconnect. |
| `AMQP_RECOVERY_TOPOLOGY` | `--recovery-topology`/`--no-recovery-topology` | `false` | Redeclare recorded topology after a reconnect. |

Boolean variables accept `true`/`false`, `yes`/`no`, `on`/`off` or `1`/`0`,
case-insensitively; any other value is rejected before the container connects
to anything (exit code 2, per `console_application.py`'s §7).
