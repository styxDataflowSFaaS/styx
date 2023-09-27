import asyncio
import dataclasses
import fractions
import struct
import socket

import zmq
from aiozmq import create_zmq_stream, ZmqStream

from .logging import logging
from .serialization import Serializer, msgpack_serialization, msgpack_deserialization, \
    cloudpickle_serialization, cloudpickle_deserialization, pickle_serialization, pickle_deserialization


class SerializerNotSupported(Exception):
    pass


@dataclasses.dataclass
class SocketConnection(object):
    zmq_socket: ZmqStream
    socket_lock: asyncio.Lock


class SocketPool(object):

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
            soc = await create_zmq_stream(zmq.DEALER, connect=f"tcp://{self.host}:{self.port}",
                                          high_read=0, high_write=0)
            soc.transport.setsockopt(zmq.LINGER, 0)
            # soc.transport.setsockopt(zmq.IMMEDIATE, 1)
            self.conns.append(SocketConnection(soc, asyncio.Lock()))

    def close(self):
        for conn in self.conns:
            conn.zmq_socket.close()
            if conn.socket_lock.locked():
                conn.socket_lock.release()
        self.conns = []


class NetworkingManager(object):

    def __init__(self):
        # HERE BETTER TO ADD A CONNECTION POOL
        self.pools: dict[tuple[str, int], SocketPool] = {}
        self.get_socket_lock = asyncio.Lock()
        self.host_name: str = str(socket.gethostbyname(socket.gethostname()))
        # event_id: ack_event
        self.waited_ack_events: dict[int, asyncio.Event] = {}
        self.ack_fraction: dict[int, fractions.Fraction] = {}
        self.aborted_events: dict[int, str] = {}

    def cleanup_after_epoch(self):
        self.waited_ack_events = {}
        self.ack_fraction = {}
        self.aborted_events = {}

    def add_ack_fraction_str(self, ack_id: int, fraction_str: str):
        try:
            self.ack_fraction[ack_id] += fractions.Fraction(fraction_str)
            logging.info(f'Ack fraction {fraction_str} received for: {ack_id} new value {self.ack_fraction[ack_id]}')
            if self.ack_fraction[ack_id] == 1:
                # All ACK parts have been gathered
                logging.info(f'All ack have been gathered for ack_id: {ack_id} {self.ack_fraction[ack_id]}')
                self.waited_ack_events[ack_id].set()
        except KeyError:
            logging.error(f'TID: {ack_id} not in ack list!')

    async def close_all_connections(self):
        async with self.get_socket_lock:
            for pool in self.pools.values():
                pool.close()
            self.pools = {}

    async def create_socket_connection(self, host: str, port):
        self.pools[(host, port)] = SocketPool(host, port)
        await self.pools[(host, port)].create_socket_connections()

    async def close_socket_connection(self, host: str, port):
        async with self.get_socket_lock:
            if (host, port) in self.pools:
                self.pools[(host, port)].close()
                del self.pools[(host, port)]
            else:
                logging.warning('The socket that you are trying to close does not exist')

    async def send_message(self,
                           host,
                           port,
                           msg: tuple,
                           msg_type: int,
                           serializer: Serializer = Serializer.CLOUDPICKLE):
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])
        msg = self.encode_message(msg, msg_type, serializer)
        socket_conn.zmq_socket.write((msg, ))

    def prepare_function_chain(self, t_id: int):
        self.waited_ack_events[t_id] = asyncio.Event()
        self.ack_fraction[t_id] = fractions.Fraction(0)

    def reset_ack_for_fallback(self, ack_ids: set[int]):
        self.cleanup_after_epoch()
        for ack_id in ack_ids:
            self.waited_ack_events[ack_id] = asyncio.Event()
            self.ack_fraction[ack_id] = fractions.Fraction(0)

    def abort_chain(self, aborted_t_id: int, exception_str: str):
        self.aborted_events[aborted_t_id] = exception_str
        self.waited_ack_events[aborted_t_id].set()

    async def __receive_message(self, sock):
        # To be used only by the request response because the lock is needed
        answer = await sock.read()
        return self.decode_message(answer[0])

    async def send_message_request_response(self,
                                            host,
                                            port,
                                            msg: tuple,
                                            msg_type: int,
                                            serializer: Serializer = Serializer.CLOUDPICKLE):
        async with self.get_socket_lock:
            if (host, port) not in self.pools:
                await self.create_socket_connection(host, port)
            socket_conn = next(self.pools[(host, port)])
        async with socket_conn.socket_lock:
            await self.__send_message_given_sock(socket_conn.zmq_socket, msg, msg_type, serializer)
            resp = await self.__receive_message(socket_conn.zmq_socket)
            logging.info("NETWORKING MODULE RECEIVED RESPONSE")
            return resp

    async def __send_message_given_sock(self, sock, msg: object, msg_type: int, serializer: Serializer):
        msg = self.encode_message(msg, msg_type, serializer)
        sock.write((msg, ))

    @staticmethod
    def encode_message(msg: object, msg_type: int, serializer: Serializer) -> bytes:
        if serializer == Serializer.CLOUDPICKLE:
            msg = struct.pack('>B', msg_type) + struct.pack('>B', 0) + cloudpickle_serialization(msg)
            return msg
        elif serializer == Serializer.MSGPACK:
            msg = struct.pack('>B', msg_type) + struct.pack('>B', 1) + msgpack_serialization(msg)
            return msg
        elif serializer == Serializer.PICKLE:
            msg = struct.pack('>B', msg_type) + struct.pack('>B', 2) + pickle_serialization(msg)
            return msg
        else:
            logging.error(f'Serializer: {serializer} is not supported')
            raise SerializerNotSupported()

    @staticmethod
    def get_msg_type(msg: bytes):
        return msg[0]

    @staticmethod
    def decode_message(data):
        serializer = data[1]
        if serializer == 0:
            msg = cloudpickle_deserialization(data[2:])
            return msg
        elif serializer == 1:
            msg = msgpack_deserialization(data[2:])
            return msg
        elif serializer == 2:
            msg = pickle_deserialization(data[2:])
            return msg
        else:
            logging.error(f'Serializer: {serializer} is not supported')
            raise SerializerNotSupported()
