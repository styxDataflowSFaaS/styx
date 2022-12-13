import asyncio
import dataclasses
import fractions
import struct
import socket

import zmq
from aiozmq import create_zmq_stream, ZmqStream

from .logging import logging
from .serialization import Serializer, msgpack_serialization, msgpack_deserialization, \
    cloudpickle_serialization, cloudpickle_deserialization


@dataclasses.dataclass
class SocketConnection:
    zmq_socket: ZmqStream
    socket_lock: asyncio.Lock


class SocketPool:

    def __init__(self, host: str, port: int, size: int = 4):
        self.host = host
        self.port = port
        self.size = size
        self.conns: list[SocketConnection] = []
        self.index: int = 0

    def __iter__(self):
        return self

    def __next__(self) -> SocketConnection:
        conn = self.conns[self.index]
        next_idx = self.index + 1
        self.index = 0 if next_idx == self.size else next_idx
        return conn

    async def create_socket_connections(self):
        for _ in range(self.size):
            soc = await create_zmq_stream(zmq.DEALER, connect=f"tcp://{self.host}:{self.port}")
            self.conns.append(SocketConnection(soc, asyncio.Lock()))

    def close(self):
        for conn in self.conns:
            conn.zmq_socket.close()
            conn.socket_lock.release()
        self.conns = []


class NetworkingManager:

    def __init__(self):
        self.pools: dict[tuple[str, int], SocketPool] = {}  # HERE BETTER TO ADD A CONNECTION POOL
        self.get_socket_lock = asyncio.Lock()
        self.host_name: str = str(socket.gethostbyname(socket.gethostname()))
        self.waited_ack_events: dict[int, asyncio.Event] = {}  # event_id: ack_event
        self.ack_fraction: dict[int, fractions.Fraction] = {}
        self.aborted_events: dict[int, str] = {}

    def cleanup_after_epoch(self):
        self.waited_ack_events = {}
        self.ack_fraction = {}
        self.aborted_events = {}

    def add_ack_fraction_str(self, ack_id: int, fraction_str: str):
        self.ack_fraction[ack_id] += fractions.Fraction(fraction_str)
        logging.info(f'Ack fraction {fraction_str} received for: {ack_id} new value {self.ack_fraction[ack_id]}')
        if self.ack_fraction[ack_id] == 1:
            # All ACK parts have been gathered
            logging.info(f'All acks have been gathered for ack_id: {ack_id} {self.ack_fraction[ack_id]}')
            self.waited_ack_events[ack_id].set()

    def close_all_connections(self):
        for pool in self.pools.values():
            pool.close()

    async def create_socket_connection(self, host: str, port):
        self.pools[(host, port)] = SocketPool(host, port)
        await self.pools[(host, port)].create_socket_connections()

    def close_socket_connection(self, host: str, port):
        if (host, port) in self.pools:
            self.pools[(host, port)].close()
        else:
            logging.warning('The socket that you are trying to close does not exist')

    async def send_message(self,
                           host,
                           port,
                           msg: dict[str, object],
                           serializer: Serializer = Serializer.CLOUDPICKLE):
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])
        msg = self.encode_message(msg, serializer)
        socket_conn.zmq_socket.write((msg, ))

    def prepare_function_chain(self, ack_share: str, t_id: int) -> tuple[str, int, str]:
        ack_payload = (self.host_name, t_id, ack_share)
        self.waited_ack_events[t_id] = asyncio.Event()
        self.ack_fraction[t_id] = fractions.Fraction(0)
        return ack_payload

    def reset_ack_for_fallback(self):
        self.aborted_events = {}
        [event.clear() for event in self.waited_ack_events.values()]
        self.ack_fraction = {t_id: fractions.Fraction(0) for t_id in self.ack_fraction.keys()}

    def abort_chain(self, aborted_t_id: int, exception_str: str):
        self.waited_ack_events[aborted_t_id].set()
        self.aborted_events[aborted_t_id] = exception_str

    async def send_message_ack(self,
                               host,
                               port,
                               ack_payload: [str, int, str],
                               msg: dict[str, object],
                               serializer: Serializer = Serializer.CLOUDPICKLE):
        msg["__ACK__"] = ack_payload
        await self.send_message(host, port, msg, serializer=serializer)

    async def __receive_message(self, sock):
        # To be used only by the request response because the lock is needed
        answer = await sock.read()
        return self.decode_message(answer[0])

    async def send_message_request_response(self,
                                            host,
                                            port,
                                            msg: dict[str, object],
                                            serializer: Serializer = Serializer.CLOUDPICKLE):
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])
        async with socket_conn.socket_lock:
            await self.__send_message_given_sock(socket_conn.zmq_socket, msg, serializer)
            resp = await self.__receive_message(socket_conn.zmq_socket)
            logging.info("NETWORKING MODULE RECEIVED RESPONSE")
            return resp

    async def __send_message_given_sock(self, sock, msg, serializer):
        msg = self.encode_message(msg, serializer)
        sock.write((msg, ))

    @staticmethod
    def encode_message(msg: object, serializer: Serializer) -> bytes:
        if serializer == Serializer.CLOUDPICKLE:
            msg = struct.pack('>H', 0) + cloudpickle_serialization(msg)
            return msg
        elif serializer == Serializer.MSGPACK:
            msg = struct.pack('>H', 1) + msgpack_serialization(msg)
            return msg
        else:
            logging.info(f'Serializer: {serializer} is not supported')

    @staticmethod
    def decode_message(data):
        serializer = struct.unpack('>H', data[0:2])[0]
        if serializer == 0:
            msg = cloudpickle_deserialization(data[2:])
            return msg
        elif serializer == 1:
            msg = msgpack_deserialization(data[2:])
            return msg
        else:
            logging.info(f'Serializer: {serializer} is not supported')
