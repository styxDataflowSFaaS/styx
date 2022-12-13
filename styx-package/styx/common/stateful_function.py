import asyncio
import fractions
import traceback
import uuid
from typing import Awaitable, Type

from .logging import logging
from .networking import NetworkingManager

from .serialization import Serializer
from .function import Function
from .base_state import BaseOperatorState as State


class StrKeyNotUUID(Exception):
    pass


class NonSupportedKeyType(Exception):
    pass


class StateNotAttachedError(Exception):
    pass


def make_key_hashable(key) -> int:
    if isinstance(key, int):
        return key
    elif isinstance(key, str):
        try:
            return uuid.UUID(key).int  # uuid type given by the user
        except ValueError:
            return uuid.uuid5(uuid.NAMESPACE_DNS, key).int  # str that we hash to SHA-1
    raise NonSupportedKeyType()  # if not int, str or uuid throw exception


class StatefulFunction(Function):

    def __init__(self,
                 key,
                 operator_name: str,
                 operator_state: State,
                 networking: NetworkingManager,
                 timestamp: int,
                 dns: dict[str, dict[str, tuple[str, int]]],
                 t_id: int,
                 request_id: str,
                 fallback_mode: bool):
        super().__init__()
        self.__operator_name = operator_name
        self.__state: State = operator_state
        self.__networking: NetworkingManager = networking
        self.__timestamp: int = timestamp
        self.__dns: dict[str, dict[str, tuple[str, int]]] = dns
        self.__t_id: int = t_id
        self.__request_id: str = request_id
        self.__async_remote_calls: list[tuple[str, str, object, tuple]] = []
        self.__fallback_enabled: bool = fallback_mode
        self.__key = key

    async def __call__(self, *args, **kwargs):
        try:
            res = await self.run(*args)
            if 'ack_share' in kwargs:
                # middle of the chain
                n_remote_calls = await self.__send_async_calls(ack_share=kwargs['ack_share'])
                return res, n_remote_calls
            elif len(self.__async_remote_calls) > 0:
                # start of the chain
                n_remote_calls = await self.__send_async_calls()
                return res, n_remote_calls
            else:
                # No chain or end
                return res, 0
        except Exception as e:
            logging.debug(traceback.format_exc())
            return e, -1

    @property
    def key(self):
        return self.__key

    async def run(self, *args):
        raise NotImplementedError

    async def get(self):
        if self.__fallback_enabled:
            value = await self.__state.get_immediate(self.key, self.__t_id, self.__operator_name)
        else:
            value = await self.__state.get(self.key, self.__t_id, self.__operator_name)
        logging.info(f'GET: {self.key}:{value} with t_id: {self.__t_id} operator: {self.__operator_name}')
        return value

    async def put(self, value):
        logging.info(f'PUT: {self.key}:{value} with t_id: {self.__t_id} operator: {self.__operator_name}')
        if self.__fallback_enabled:
            await self.__state.put_immediate(self.key, value, self.__t_id, self.__operator_name)
        else:
            await self.__state.put(self.key, value, self.__t_id, self.__operator_name)

    async def __send_async_calls(self, ack_share=1):
        n_remote_calls: int = len(self.__async_remote_calls)
        if n_remote_calls > 0 and not self.__fallback_enabled:
            # if fallback is enabled there is no need to call the functions because they are already cached
            ack_payload = self.__networking.prepare_function_chain(
                str(fractions.Fraction(f'1/{n_remote_calls}') * fractions.Fraction(ack_share)), self.__t_id)
            remote_calls: list[Awaitable] = [self.call_remote_function_no_response(*entry, ack_payload=ack_payload)
                                             for entry in self.__async_remote_calls]
            logging.info(f'Sending chain calls for function: {self.name} with remote call number: {n_remote_calls}'
                         f'and calls: {self.__async_remote_calls}')
            await asyncio.gather(*remote_calls)
        return n_remote_calls

    def call_remote_async(self,
                          operator_name: str,
                          function_name: Type | str,
                          key,
                          params: tuple = tuple()):
        if isinstance(function_name, type):
            function_name = function_name.__name__
        self.__async_remote_calls.append((operator_name, function_name, key, params))

    async def call_remote_function_no_response(self,
                                               operator_name: str,
                                               function_name: Type | str, key,
                                               params: tuple = tuple(),
                                               ack_payload=None):
        if isinstance(function_name, type):
            function_name = function_name.__name__
        partition, payload, operator_host, operator_port = self.__prepare_message_transmission(operator_name,
                                                                                               key,
                                                                                               function_name,
                                                                                               params)

        await self.__networking.send_message_ack(operator_host,
                                                 operator_port,
                                                 ack_payload,
                                                 {"__COM_TYPE__": 'RUN_FUN_REMOTE',
                                                  "__MSG__": payload},
                                                 Serializer.MSGPACK)

    async def call_remote_function_request_response(self,
                                                    operator_name: str,
                                                    function_name:  Type | str,
                                                    key,
                                                    params: tuple = tuple()):
        if isinstance(function_name, type):
            function_name = function_name.__name__
        partition, payload, operator_host, operator_port = self.__prepare_message_transmission(operator_name,
                                                                                               key,
                                                                                               function_name,
                                                                                               params)
        logging.info(f'(SF)  Start {operator_host}:{operator_port} of {operator_name}:{partition}')
        resp = await self.__networking.send_message_request_response(operator_host,
                                                                     operator_port,
                                                                     {"__COM_TYPE__": 'RUN_FUN_RQ_RS_REMOTE',
                                                                      "__MSG__": payload},
                                                                     Serializer.MSGPACK)
        return resp

    def __prepare_message_transmission(self, operator_name: str, key, function_name: str, params: tuple):
        try:
            partition: int = make_key_hashable(key) % len(self.__dns[operator_name].keys())

            payload = {'__T_ID__': self.__t_id,
                       '__RQ_ID__': self.__request_id,
                       '__OP_NAME__': operator_name,
                       '__FUN_NAME__': function_name,
                       '__KEY__': key,
                       '__PARTITION__': partition,
                       '__TIMESTAMP__': self.__timestamp,
                       '__PARAMS__': params}

            operator_host = self.__dns[operator_name][str(partition)][0]
            operator_port = self.__dns[operator_name][str(partition)][1]
        except KeyError:
            logging.error(f"Couldn't find operator: {operator_name} in {self.__dns}")
        else:
            return partition, payload, operator_host, operator_port
