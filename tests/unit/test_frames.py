"""Unit tests for AMQP 1.0 framing over a socket."""

from __future__ import annotations

import socket
import struct
import threading
import time

import pytest

from rabbitmq_amqp_python_client.exceptions import ProtocolError
from rabbitmq_amqp_python_client.wire import frames, sasl
from rabbitmq_amqp_python_client.wire import performatives as perf


@pytest.fixture
def socket_pair():
    """Yield a connected ``(client, server)`` socket pair, closed on teardown."""
    left, right = socket.socketpair()
    left.settimeout(5.0)
    right.settimeout(5.0)
    try:
        yield left, right
    finally:
        left.close()
        right.close()


class TestBuildFrame:
    def test_header_layout(self):
        body = b"\x01\x02\x03"
        frame = frames.build_frame(frames.FRAME_TYPE_AMQP, 7, body)
        size, doff, frame_type, channel = struct.unpack(">IBBH", frame[:8])
        assert size == len(frame) == frames.FRAME_HEADER_SIZE + len(body)
        assert doff == frames.DEFAULT_DOFF
        assert frame_type == frames.FRAME_TYPE_AMQP
        assert channel == 7
        assert frame[8:] == body

    def test_empty_frame_is_eight_bytes(self):
        frame = frames.build_frame(frames.FRAME_TYPE_AMQP, 0)
        assert frame == b"\x00\x00\x00\x08\x02\x00\x00\x00"

    def test_heartbeat_constant_is_an_empty_amqp_frame(self):
        assert frames.build_frame(frames.FRAME_TYPE_AMQP, 0) == frames.EMPTY_FRAME

    def test_sasl_frames_use_frame_type_one(self):
        frame = frames.build_frame(frames.FRAME_TYPE_SASL, 0, b"\x40")
        assert frame[5] == 0x01

    def test_size_is_big_endian(self):
        frame = frames.build_frame(frames.FRAME_TYPE_AMQP, 0, b"x" * 256)
        assert frame[:4] == b"\x00\x00\x01\x08"


class TestReadFrame:
    def test_reads_a_normal_frame(self, socket_pair):
        left, right = socket_pair
        body = perf.Open(container_id="client-1").encode()
        right.sendall(frames.build_frame(frames.FRAME_TYPE_AMQP, 3, body))
        assert frames.read_frame(left) == (frames.FRAME_TYPE_AMQP, 3, body)

    def test_reads_a_heartbeat_with_an_empty_body(self, socket_pair):
        left, right = socket_pair
        right.sendall(frames.EMPTY_FRAME)
        frame_type, channel, body = frames.read_frame(left)
        assert (frame_type, channel) == (frames.FRAME_TYPE_AMQP, 0)
        assert body == b""

    def test_reads_a_sasl_frame(self, socket_pair):
        left, right = socket_pair
        body = sasl.SaslMechanisms(["PLAIN"]).encode()
        right.sendall(frames.build_frame(frames.FRAME_TYPE_SASL, 0, body))
        assert frames.read_frame(left) == (frames.FRAME_TYPE_SASL, 0, body)

    def test_reads_consecutive_frames_from_one_write(self, socket_pair):
        left, right = socket_pair
        first = frames.build_frame(frames.FRAME_TYPE_AMQP, 0, perf.End().encode())
        second = frames.build_frame(frames.FRAME_TYPE_AMQP, 1, perf.Close().encode())
        right.sendall(first + second)
        assert frames.read_frame(left)[1] == 0
        assert frames.read_frame(left)[1] == 1

    def test_reads_a_frame_split_across_many_recv_calls(self, socket_pair):
        left, right = socket_pair
        body = perf.Transfer(handle=0, delivery_id=1, delivery_tag=b"\x01").encode() + b"y" * 4096
        frame = frames.build_frame(frames.FRAME_TYPE_AMQP, 0, body)

        def feed_in_small_chunks() -> None:
            for index in range(0, len(frame), 7):
                right.sendall(frame[index : index + 7])
                time.sleep(0.001)

        writer = threading.Thread(target=feed_in_small_chunks)
        writer.start()
        try:
            assert frames.read_frame(left) == (frames.FRAME_TYPE_AMQP, 0, body)
        finally:
            writer.join(5.0)
        assert not writer.is_alive()

    def test_skips_an_extended_header(self, socket_pair):
        left, right = socket_pair
        body = perf.Close().encode()
        extended = b"\xaa\xbb\xcc\xdd"
        size = frames.FRAME_HEADER_SIZE + len(extended) + len(body)
        right.sendall(struct.pack(">IBBH", size, 3, frames.FRAME_TYPE_AMQP, 0) + extended + body)
        assert frames.read_frame(left) == (frames.FRAME_TYPE_AMQP, 0, body)

    def test_close_before_any_byte_raises_protocol_error(self, socket_pair):
        left, right = socket_pair
        right.close()
        with pytest.raises(ProtocolError, match="peer closed the connection after 0 of 4"):
            frames.read_frame(left)

    def test_close_mid_frame_raises_protocol_error(self, socket_pair):
        left, right = socket_pair
        body = perf.Open(container_id="client-1").encode()
        frame = frames.build_frame(frames.FRAME_TYPE_AMQP, 0, body)
        right.sendall(frame[: len(frame) - 5])
        right.close()
        with pytest.raises(ProtocolError, match="peer closed the connection after"):
            frames.read_frame(left)

    def test_close_after_the_size_field_raises_protocol_error(self, socket_pair):
        left, right = socket_pair
        right.sendall(struct.pack(">I", 64))
        right.close()
        with pytest.raises(ProtocolError, match="of 60 expected bytes"):
            frames.read_frame(left)

    def test_undersized_frame_is_rejected(self, socket_pair):
        left, right = socket_pair
        right.sendall(struct.pack(">I", 4))
        with pytest.raises(ProtocolError, match="smaller than the 8-byte frame header"):
            frames.read_frame(left)

    def test_oversized_frame_is_rejected(self, socket_pair):
        left, right = socket_pair
        right.sendall(struct.pack(">I", frames.FRAME_SIZE_LIMIT + 1))
        with pytest.raises(ProtocolError, match="exceeds the"):
            frames.read_frame(left)

    def test_data_offset_below_the_minimum_is_rejected(self, socket_pair):
        left, right = socket_pair
        right.sendall(struct.pack(">IBBH", 8, 1, frames.FRAME_TYPE_AMQP, 0))
        with pytest.raises(ProtocolError, match="below the minimum"):
            frames.read_frame(left)

    def test_data_offset_past_the_frame_end_is_rejected(self, socket_pair):
        left, right = socket_pair
        right.sendall(struct.pack(">IBBH", 8, 4, frames.FRAME_TYPE_AMQP, 0))
        with pytest.raises(ProtocolError, match="overruns"):
            frames.read_frame(left)

    def test_a_protocol_header_where_a_frame_was_expected_is_rejected(self, socket_pair):
        left, right = socket_pair
        right.sendall(sasl.AMQP_PROTOCOL_HEADER)
        with pytest.raises(ProtocolError, match="received protocol header"):
            frames.read_frame(left)


class TestRecvExactly:
    def test_zero_bytes_does_not_touch_the_socket(self, socket_pair):
        left, _ = socket_pair
        assert frames.recv_exactly(left, 0) == b""

    def test_accumulates_short_reads(self):
        class ChunkedSource:
            def __init__(self, chunks: list[bytes]) -> None:
                self.chunks = list(chunks)
                self.calls = 0

            def recv(self, buffer_size: int) -> bytes:
                self.calls += 1
                return self.chunks.pop(0)[:buffer_size]

        source = ChunkedSource([b"ab", b"cd", b"ef"])
        assert frames.recv_exactly(source, 6) == b"abcdef"
        assert source.calls == 3

    def test_socket_errors_are_wrapped(self):
        class FailingSource:
            def recv(self, buffer_size: int) -> bytes:
                raise OSError("connection reset")

        with pytest.raises(ProtocolError, match="socket error after 0 of 4 bytes") as excinfo:
            frames.recv_exactly(FailingSource(), 4)
        assert isinstance(excinfo.value.__cause__, OSError)


class TestWriteFrame:
    def test_round_trips_through_a_socket_pair(self, socket_pair):
        left, right = socket_pair
        body = perf.Begin(next_outgoing_id=0, incoming_window=8, outgoing_window=8).encode()
        frames.write_frame(left, frames.FRAME_TYPE_AMQP, 2, body)
        assert frames.read_frame(right) == (frames.FRAME_TYPE_AMQP, 2, body)

    def test_writes_a_heartbeat(self, socket_pair):
        left, right = socket_pair
        frames.write_frame(left, frames.FRAME_TYPE_AMQP, 0)
        assert frames.read_frame(right) == (frames.FRAME_TYPE_AMQP, 0, b"")

    def test_writes_a_large_body_in_one_call(self, socket_pair):
        left, right = socket_pair
        body = perf.Transfer(handle=0, delivery_id=1).encode() + b"z" * 200000
        writer = threading.Thread(target=frames.write_frame, args=(left, frames.FRAME_TYPE_AMQP, 0, body))
        writer.start()
        try:
            assert frames.read_frame(right) == (frames.FRAME_TYPE_AMQP, 0, body)
        finally:
            writer.join(5.0)
        assert not writer.is_alive()

    def test_socket_errors_are_wrapped(self, socket_pair):
        left, right = socket_pair
        right.close()
        left.close()
        with pytest.raises(ProtocolError, match="socket error while writing"):
            frames.write_frame(left, frames.FRAME_TYPE_AMQP, 0, b"\x40")


class TestProtocolHeaders:
    def test_write_then_read(self, socket_pair):
        left, right = socket_pair
        frames.write_protocol_header(left, sasl.AMQP_SASL_HEADER)
        assert frames.read_protocol_header(right) == sasl.AMQP_SASL_HEADER

    def test_reads_a_header_split_across_recv_calls(self, socket_pair):
        left, right = socket_pair
        right.sendall(sasl.AMQP_PROTOCOL_HEADER[:3])

        def send_rest() -> None:
            time.sleep(0.01)
            right.sendall(sasl.AMQP_PROTOCOL_HEADER[3:])

        writer = threading.Thread(target=send_rest)
        writer.start()
        try:
            assert frames.read_protocol_header(left) == sasl.AMQP_PROTOCOL_HEADER
        finally:
            writer.join(5.0)

    def test_a_non_amqp_header_is_rejected(self, socket_pair):
        left, right = socket_pair
        right.sendall(b"HTTP/1.1")
        with pytest.raises(ProtocolError, match="invalid protocol header"):
            frames.read_protocol_header(left)

    def test_a_wrong_length_header_is_rejected(self, socket_pair):
        left, _ = socket_pair
        with pytest.raises(ProtocolError, match="must be 8 bytes"):
            frames.write_protocol_header(left, b"AMQP")

    def test_close_mid_header_raises_protocol_error(self, socket_pair):
        left, right = socket_pair
        right.sendall(b"AMQP")
        right.close()
        with pytest.raises(ProtocolError, match="peer closed the connection after 4 of 8"):
            frames.read_protocol_header(left)


class TestDecodeFrameBody:
    def test_empty_body_is_a_heartbeat(self):
        assert frames.decode_frame_body(frames.FRAME_TYPE_AMQP, b"") == (None, b"")

    def test_amqp_body_decodes_to_a_performative(self):
        performative = perf.Open(container_id="client-1", hostname="vhost")
        body, payload = frames.decode_frame_body(frames.FRAME_TYPE_AMQP, performative.encode())
        assert body == performative
        assert payload == b""

    def test_transfer_body_splits_off_the_payload(self):
        performative = perf.Transfer(handle=1, delivery_id=4, delivery_tag=b"\x04")
        raw_payload = b"encoded-message-bytes"
        body, payload = frames.decode_frame_body(frames.FRAME_TYPE_AMQP, performative.encode() + raw_payload)
        assert body == performative
        assert payload == raw_payload

    def test_sasl_body_decodes_to_a_sasl_frame(self):
        frame_body = sasl.SaslOutcome(code=sasl.SASL_OK)
        body, payload = frames.decode_frame_body(frames.FRAME_TYPE_SASL, frame_body.encode())
        assert body == frame_body
        assert payload == b""

    def test_unknown_frame_type_is_rejected(self):
        with pytest.raises(ProtocolError, match="unknown frame type 0x02"):
            frames.decode_frame_body(0x02, b"\x40")

    def test_end_to_end_read_and_decode(self, socket_pair):
        left, right = socket_pair
        performative = perf.Transfer(handle=0, delivery_id=1, delivery_tag=b"\x01", settled=True)
        payload = b"the-message"
        frames.write_frame(right, frames.FRAME_TYPE_AMQP, 5, performative.encode() + payload)
        frame_type, channel, body = frames.read_frame(left)
        assert channel == 5
        assert frames.decode_frame_body(frame_type, body) == (performative, payload)
