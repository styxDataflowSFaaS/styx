import time
import types
import uuid
from typing import Type

import cloudpickle
from aiokafka import AIOKafkaProducer
from kafka import KafkaProducer
from kafka.errors import KafkaConnectionError

from .common.serialization import Serializer, msgpack_serialization, \
    cloudpickle_serialization, cloudpickle_deserialization
from .common.logging import logging
from .common.networking import NetworkingManager
from .common.stateflow_graph import StateflowGraph
from .common.stateflow_ingress import IngressTypes
from .common.operator import BaseOperator
from .common.stateful_function import make_key_hashable


class NotAStateflowGraph(Exception):
    pass


class GraphNotSerializable(Exception):
    pass


class Styx(object):

    kafka_producer: AIOKafkaProducer

    def __init__(self,
                 coordinator_adr: str,
                 coordinator_port: int,
                 ingress_type: IngressTypes,
                 kafka_url: str = None):
        self.coordinator_adr = coordinator_adr
        self.coordinator_port = coordinator_port
        self.networking_manager = NetworkingManager()
        if ingress_type == IngressTypes.KAFKA:
            self.kafka_url = kafka_url
            self.sync_kafka_producer: KafkaProducer = KafkaProducer(bootstrap_servers=self.kafka_url,
                                                                    acks='all')

    @staticmethod
    def get_modules(stateflow_graph: StateflowGraph):
        modules = {types.ModuleType(stateflow_graph.__module__)}
        for operator in stateflow_graph.nodes.values():
            modules.add(types.ModuleType(operator.__module__))
            for function in operator.functions.values():
                modules.add(types.ModuleType(function.__module__))
        return modules

    @staticmethod
    def check_serializability(stateflow_graph):
        try:
            ser = cloudpickle_serialization(stateflow_graph)
            cloudpickle_deserialization(ser)
        except Exception:
            raise GraphNotSerializable("The submitted graph is not serializable, "
                                       "all external modules should be declared")

    async def submit(self, stateflow_graph: StateflowGraph, external_modules: tuple = None):
        logging.info(f'Submitting Stateflow graph: {stateflow_graph.name}')
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        modules = self.get_modules(stateflow_graph)
        system_module_name = __name__.split('.')[0]
        for module in modules:
            # exclude system modules
            if not module.__name__.startswith(system_module_name) and \
                    not module.__name__.startswith("stateflow"):
                cloudpickle.register_pickle_by_value(module)
        if external_modules is not None:
            for external_module in external_modules:
                cloudpickle.register_pickle_by_value(external_module)

        self.check_serializability(stateflow_graph)
        print("Sending execution graph")
        await self.send_execution_graph(stateflow_graph)
        logging.info(f'Submission of Stateflow graph: {stateflow_graph.name} completed')

    async def send_kafka_event(self,
                               operator: BaseOperator,
                               key,
                               function: Type | str,
                               params: tuple = tuple(),
                               serializer: Serializer = Serializer.MSGPACK):
        request_id, serialized_value, partition = self.__prepare_kafka_message(key,
                                                                               operator,
                                                                               function,
                                                                               params,
                                                                               serializer)
        msg = await self.kafka_producer.send_and_wait(operator.name,
                                                      key=msgpack_serialization(request_id),
                                                      value=serialized_value,
                                                      partition=partition)
        return request_id, msg.timestamp

    def send_kafka_event_no_wait(self,
                                 operator: BaseOperator,
                                 key,
                                 function: Type | str,
                                 params: tuple = tuple(),
                                 serializer: Serializer = Serializer.MSGPACK):
        request_id, serialized_value, partition = self.__prepare_kafka_message(key,
                                                                               operator,
                                                                               function,
                                                                               params,
                                                                               serializer)
        metadata = self.sync_kafka_producer.send(operator.name,
                                                 key=msgpack_serialization(request_id),
                                                 value=serialized_value,
                                                 partition=partition)
        return request_id, metadata

    def __prepare_kafka_message(self, key, operator, function, params, serializer):
        partition: int = make_key_hashable(key) % operator.n_partitions
        fun_name: str = function if isinstance(function, str) else function.__name__
        event = (operator.name,  # __OP_NAME__
                 key,  # __KEY__
                 fun_name,  # __FUN_NAME__
                 params,  # __PARAMS__
                 partition)  # __PARTITION__
        request_id = uuid.uuid1().int >> 64
        serialized_value: bytes = self.networking_manager.encode_message(msg=event,
                                                                         msg_type=16,
                                                                         serializer=serializer)
        return request_id, serialized_value, partition

    async def start_kafka_producer(self):
        self.kafka_producer = AIOKafkaProducer(bootstrap_servers=[self.kafka_url],
                                               enable_idempotence=True)
        while True:
            try:
                await self.kafka_producer.start()
            except KafkaConnectionError:
                time.sleep(1)
                logging.info("Waiting for Kafka")
                continue
            break
        logging.info('KAFKA PRODUCER STARTED')

    async def send_execution_graph(self, stateflow_graph: StateflowGraph):
        await self.networking_manager.send_message(self.coordinator_adr,
                                                   self.coordinator_port,
                                                   msg=(stateflow_graph, ),
                                                   msg_type=2)

    async def start(self):
        await self.start_kafka_producer()

    async def close(self):
        await self.kafka_producer.stop()
