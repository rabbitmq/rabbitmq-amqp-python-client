import urllib.parse

import requests
from requests.auth import HTTPBasicAuth


def get_connections_names() -> list:
    request = "http://localhost:15672/api/connections"
    responses = requests.get(request, auth=HTTPBasicAuth("guest", "guest"))
    responses.raise_for_status()
    connections = responses.json()
    connection_names = []
    for connection in connections:
        connection_names.append(connection["name"])
    return connection_names


def delete_connections(connection_names: []) -> None:
    for connection_name in connection_names:
        request = (
            "http://guest:guest@localhost:15672/api/connections/"
            + urllib.parse.quote(connection_name)
        )
        requests.delete(request, auth=HTTPBasicAuth("guest", "guest"))


def delete_all_connections() -> None:
    connection_names = get_connections_names()
    delete_connections(connection_names)