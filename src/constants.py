"""RabbitMQ/AMQP symbol names and other wire-level constants used across the client."""

from __future__ import annotations

# --- Management ---
MANAGEMENT_NODE_ADDRESS = "/management"
MANAGEMENT_LINK_NAME = "management-link-pair"
MANAGEMENT_REPLY_TO = "$me"
MANAGEMENT_LINK_CREDIT = 100

# --- Queue/exchange address prefixes ---
QUEUE_ADDRESS_TEMPLATE = "/queues/{name}"
EXCHANGE_ADDRESS_TEMPLATE = "/exchanges/{name}"
EXCHANGE_ADDRESS_WITH_KEY_TEMPLATE = "/exchanges/{name}/{key}"

# --- Single active consumer (quorum queues) ---
RABBITMQ_ACTIVE_PROPERTY = "rabbitmq:active"

# --- Direct reply-to (step_060 §3.3) ---
DIRECT_REPLY_TO_CAPABILITY = "rabbitmq:volatile-queue"

# --- Stream offset / filtering symbols (Source.filter map keys) ---
STREAM_OFFSET_SPEC_FILTER = "rabbitmq:stream-offset-spec"
STREAM_FILTER_VALUES_FILTER = "rabbitmq:stream-filter"
STREAM_MATCH_UNFILTERED_FILTER = "rabbitmq:stream-match-unfiltered"
AMQP_PROPERTIES_FILTER = "amqp:properties-filter"
AMQP_APPLICATION_PROPERTIES_FILTER = "amqp:application-properties-filter"
AMQP_SQL_FILTER = "amqp:sql-filter"

# RabbitMQ matches the SQL filter by the *name* it has in the filter set, which
# is this short one, and only then checks that the described value carries the
# AMQP_SQL_FILTER descriptor; naming the entry "amqp:sql-filter" instead makes
# the broker fall through to its property-filter parser and drop the filter.
SQL_FILTER_NAME = "sql-filter"

# --- Stream message annotations ---
STREAM_FILTER_VALUE_ANNOTATION = "x-stream-filter-value"
STREAM_OFFSET_ANNOTATION = "x-stream-offset"

# --- Performance test / console application ---
SEND_TIMESTAMP_PROPERTY = "x-send-timestamp"

DEFAULT_HOST = "localhost"
DEFAULT_AMQP_PORT = 5672
DEFAULT_AMQPS_PORT = 5671
DEFAULT_VIRTUAL_HOST = "/"
DEFAULT_USER = "guest"
DEFAULT_PASSWORD = "guest"
DEFAULT_MAX_FRAME_SIZE = 1024 * 1024
DEFAULT_CHANNEL_MAX = 65535
DEFAULT_IDLE_TIMEOUT_MS = 0
