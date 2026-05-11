import logging
import struct
from typing import Callable, Optional

log = logging.getLogger(__name__)

_AMQP_FRAME_TYPE = 0x00
_FLOW_DESCRIPTOR_CODE = 0x13  # AMQP 1.0 FLOW performative
_RABBITMQ_ACTIVE_KEY = "rabbitmq:active"

# Key used to attach the SAC callback to a Transport object.
# Set via setattr(transport, SAC_CALLBACK_KEY, callback) and
# read back in IOHandler.on_selectable_readable.
SAC_CALLBACK_KEY = "_rabbitmq_sac_callback"

SACStateCallback = Callable[[bool], None]


def extract_sac_active_from_bytes(raw_bytes: bytes) -> Optional[bool]:
    """
    Scan a chunk of raw AMQP bytes for FLOW frames that carry the
    ``rabbitmq:active`` link-state property.

    Parses every complete AMQP frame in *raw_bytes* and returns the boolean
    value of ``rabbitmq:active`` from the first FLOW frame that contains it.
    Incomplete frames at the end of the buffer are silently skipped (they are
    rare for small FLOW frames and will be retried on the next read).

    Returns:
        ``True`` / ``False`` if a matching FLOW frame was found, ``None`` otherwise.
    """
    offset = 0
    while offset + 8 <= len(raw_bytes):
        try:
            frame_size = struct.unpack_from(">I", raw_bytes, offset)[0]
            if frame_size < 8:
                break
            if offset + frame_size > len(raw_bytes):
                break

            doff = raw_bytes[offset + 4]
            frame_type = raw_bytes[offset + 5]

            if frame_type == _AMQP_FRAME_TYPE and doff >= 2:
                body_start = offset + doff * 4
                body_end = offset + frame_size
                if body_start < body_end:
                    result = _decode_flow_active(raw_bytes[body_start:body_end])
                    if result is not None:
                        return result
        except Exception:
            pass

        offset += frame_size

    return None


def _decode_flow_active(body: bytes) -> Optional[bool]:
    """Decode an AMQP performative body and return ``rabbitmq:active`` if present."""
    from cproton import (
        PN_BOOL,
        PN_DESCRIBED,
        PN_LIST,
        PN_MAP,
        PN_SYMBOL,
        PN_ULONG,
        pn_data,
        pn_data_decode,
        pn_data_enter,
        pn_data_exit,
        pn_data_free,
        pn_data_get_bool,
        pn_data_get_list,
        pn_data_get_map,
        pn_data_get_symbol,
        pn_data_get_ulong,
        pn_data_next,
        pn_data_rewind,
        pn_data_type,
    )

    if not body:
        return None

    data = pn_data(len(body) + 64)
    try:
        if pn_data_decode(data, body) <= 0:
            return None

        pn_data_rewind(data)

        if not pn_data_next(data) or pn_data_type(data) != PN_DESCRIBED:
            return None

        pn_data_enter(data)

        if not pn_data_next(data) or pn_data_type(data) != PN_ULONG:
            return None
        if pn_data_get_ulong(data) != _FLOW_DESCRIPTOR_CODE:
            return None

        if not pn_data_next(data) or pn_data_type(data) != PN_LIST:
            return None

        flow_count = pn_data_get_list(data)
        if flow_count <= 10:
            return None

        pn_data_enter(data)

        for i in range(11):
            if not pn_data_next(data):
                return None
            if i == 10:
                if pn_data_type(data) != PN_MAP:
                    return None
                num_entries = pn_data_get_map(data)
                pn_data_enter(data)
                for _ in range(num_entries // 2):
                    if not pn_data_next(data):
                        break
                    if pn_data_type(data) == PN_SYMBOL:
                        key = pn_data_get_symbol(data)
                        if not pn_data_next(data):
                            break
                        if (
                            key == _RABBITMQ_ACTIVE_KEY
                            and pn_data_type(data) == PN_BOOL
                        ):
                            active = bool(pn_data_get_bool(data))
                            pn_data_exit(data)
                            return active
                    else:
                        pn_data_next(data)
                pn_data_exit(data)
                return None
    except Exception as exc:
        log.debug("Error decoding FLOW frame body: %s", exc)
        return None
    finally:
        pn_data_free(data)
