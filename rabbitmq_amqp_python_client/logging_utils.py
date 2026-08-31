"""Logging helpers.

The client logs through the standard :mod:`logging` package under the
``rabbitmq_amqp_python_client`` logger namespace. Applications configure
levels/handlers the normal way (``logging.getLogger("rabbitmq_amqp_python_client")``);
this module only centralizes the child-logger naming convention.
"""

from __future__ import annotations

import logging

ROOT_LOGGER_NAME = "rabbitmq_amqp_python_client"


def get_logger(component: str) -> logging.Logger:
    """Return the logger for a given client component.

    Args:
        component: Dotted suffix identifying the component, e.g. ``"connection"``
            or ``"consumer"``.

    Returns:
        A logger named ``rabbitmq_amqp_python_client.<component>``.
    """
    return logging.getLogger(f"{ROOT_LOGGER_NAME}.{component}")
