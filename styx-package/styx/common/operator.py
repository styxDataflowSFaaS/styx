import logging

from .networking import NetworkingManager
from .serialization import Serializer, msgpack_deserialization
from .base_operator import BaseOperator
from .stateful_function import StatefulFunction

SERVER_PORT = 8889


class NotAFunctionError(Exception):
    pass


class OperatorDoesNotContainFunction(Exception):
    pass


class Operator(BaseOperator):

    def __init__(self,
                 name: str,
                 n_partitions: int = 1):
        super().__init__(name, n_partitions)
        self.__state = ...
        self.__networking: NetworkingManager = ...
        # where the other functions exist
        self.__dns: dict[str, dict[str, tuple[str, int]]] = {}
        self.__functions: dict[str, type] = {}

    @property
    def functions(self):
        return self.__functions

    async def run_function(self,
                           key,
                           t_id: int,
                           request_id: bytes,
                           timestamp: int,
                           function_name: str,
                           ack_payload: tuple[str, int, str] | None,
                           fallback_mode: bool,
                           params: tuple) -> tuple[any, bool]:
        logging.info(f'RQ_ID: {msgpack_deserialization(request_id)} TID: {t_id} '
                     f'function: {function_name} fallback mode: {fallback_mode}')
        f = self.__materialize_function(function_name, key, t_id, request_id, timestamp, fallback_mode)
        params = (f, ) + tuple(params)
        if ack_payload is not None:
            # part of a chain (not root)
            ack_host, ack_id, fraction_str = ack_payload
            resp, n_remote_calls = await f(*params, ack_host=ack_host, ack_share=fraction_str)
            if not isinstance(resp, Exception):
                if n_remote_calls == 0:
                    # final link of the chain (send ack share)
                    await self.__networking.send_message(ack_host, SERVER_PORT,
                                                         msg=(ack_id, fraction_str, ""),
                                                         msg_type=3,
                                                         serializer=Serializer.MSGPACK)
                    # logging.warning(f"Sending ack: {ack_id}")
            else:
                # Send chain failure
                await self.__networking.send_message(ack_host, SERVER_PORT,
                                                     msg=(ack_id, '-1', str(resp)),
                                                     msg_type=3,
                                                     serializer=Serializer.MSGPACK)
                # logging.warning(f"Sending ack: {ack_id}")
        else:
            # root of a chain, or single call
            resp, _ = await f(*params)
        del f
        return resp

    def __materialize_function(self, function_name, key, t_id, request_id, timestamp, fallback_mode):
        f = StatefulFunction(key,
                             self.name,
                             self.__state,
                             self.__networking,
                             timestamp,
                             self.__dns,
                             t_id,
                             request_id,
                             fallback_mode)
        try:
            f.run = self.__functions[function_name]
        except KeyError:
            raise OperatorDoesNotContainFunction(f'Operator: {self.name} does not contain function: {function_name}')
        return f

    def register(self, func: type):
        self.__functions[func.__name__] = func

    def attach_state_networking(self, state, networking, dns):
        self.__state = state
        self.__networking = networking
        self.__dns = dns
