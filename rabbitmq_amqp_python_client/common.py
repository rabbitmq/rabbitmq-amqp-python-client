import enum


class CommonValues(enum.Enum):
    response_code_200 = 200
    response_code_201 = 201
    response_code_204 = 204
    response_code_404 = 404
    response_code_409 = 409
    command_put = "PUT"
    command_get = "GET"
    command_post = "POST"
    command_delete = "DELETE"
    command_reply_to = "$me"
    management_node_address = "/management"
    link_pair_name = "management-link-pair"
    exchanges = "exchanges"
    key = "key"
    queue = "queues"
    bindings = "bindings"


class ExchangeType(enum.Enum):
    direct = "direct"
    topic = "topic"
    fanout = "fanout"


class QueueType(enum.Enum):
    quorum = "quorum"
    classic = "classic"
    stream = "stream"
