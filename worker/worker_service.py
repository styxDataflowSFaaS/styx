import asyncio
import os
import time

import aiozmq
import uvloop
import zmq
from aiokafka import TopicPartition
from aiozmq import ZmqStream
from minio import Minio

from timeit import default_timer as timer

from styx.common.local_state_backends import LocalStateBackend
from styx.common.logging import logging
from styx.common.networking import NetworkingManager
from styx.common.operator import Operator
from styx.common.protocols import Protocols
from styx.common.serialization import Serializer

from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.operator_state.aria.incremental_operator_state import IncrementalOperatorState, KVTypes
from operator_state.stateless import Stateless

from worker.fault_tolerance.async_snapshots import AsyncSnapshotsMinio
from worker.fault_tolerance.incremental_snapshots import IncrementalSnapshotsMinio
from worker.operator_state.unsafe_state import UnsafeOperatorState
from worker.transactional_protocols.aria import AriaProtocol
from worker.transactional_protocols.base_protocol import PROTOCOL_PORT
from worker.transactional_protocols.unsafe import UnsafeProtocol
from worker.util.aio_task_scheduler import AIOTaskScheduler

SERVER_PORT: int = 8888
DISCOVERY_HOST: str = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT: int = int(os.environ['DISCOVERY_PORT'])
INGRESS_TYPE = os.getenv('INGRESS_TYPE', None)

MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']

HEARTBEAT_INTERVAL: int = int(os.getenv('HEARTBEAT_INTERVAL', 100))  # 100ms

PROTOCOL = Protocols.Aria

# TODO should we consider the order in the chain?
# TODO assume that they arrive in order, need to verify (why is this an issue?)
# TODO add AIOKafkaAdmin in the coordinator when it becomes stable i.e., aiokafka > 0.8.0
# TODO Check dynamic r/w sets
# TODO garbage collect the snapshots
# TODO in case of failure we probably need to clear 0mq to avoid inconsistencies (need to check)
# TODO check why recovery is taking more time than when the stared happens
# TODO Fix function caching
# TODO check parquet for snapshot store


class Worker(object):

    def __init__(self):
        self.id: int = -1
        self.networking = NetworkingManager()
        self.router: ZmqStream = ...
        self.protocol_router: ZmqStream = ...

        self.operator_state_backend: LocalStateBackend = ...
        self.registered_operators: dict[tuple[str, int], Operator] = {}
        self.dns: dict[str, dict[str, tuple[str, int]]] = {}
        self.topic_partitions: list[TopicPartition] = []
        # worker_id: (host, port)
        self.peers: dict[int, tuple[str, int]] = {}
        self.local_state: InMemoryOperatorState | Stateless | IncrementalOperatorState = ...

        # Primary tasks used for processing
        self.function_scheduler_task: asyncio.Task = ...
        self.heartbeat_task: asyncio.Task = ...

        self.function_execution_protocol: AriaProtocol = ...

        self.aio_task_scheduler = AIOTaskScheduler()

        self.async_snapshots: AsyncSnapshotsMinio | IncrementalSnapshotsMinio = ...

        # performance measurements
        # self.function_running_time = 0
        # self.chain_completion_time = 0
        # self.serialization_time = 0
        # self.sequencing_time = 0
        # self.conflict_resolution_time = 0
        # self.commit_time = 0
        # self.sync_time = 0
        # self.snapshot_time = 0
        # self.fallback_time = 0

    async def worker_controller(self, data: bytes):
        message_type: int = self.networking.get_msg_type(data)
        match message_type:
            # RECEIVE EXECUTION PLAN OF A DATAFLOW GRAPH
            case 4:
                logging.warning('received case 4 message')
                # This contains all the operators of a job assigned to this worker
                message = self.networking.decode_message(data)
                await self.handle_execution_plan(message)
            case 20:
                # RECOVERY_OTHER
                start_time = timer()
                logging.warning('RECOVERY_OTHER')
                await self.networking.close_all_connections()

                self.networking = NetworkingManager()

                message = self.networking.decode_message(data)
                self.dns, self.peers, self.operator_state_backend, snapshot_id = message
                del self.peers[self.id]

                metadata: dict | None = None
                if self.operator_state_backend is LocalStateBackend.DICT:
                    state, metadata = self.async_snapshots.retrieve_snapshot(snapshot_id)
                    self.attach_state_to_operators_after_snapshot(state)
                elif self.operator_state_backend in [LocalStateBackend.HYBRID_LOG,
                                                     LocalStateBackend.LSM,
                                                     LocalStateBackend.APPEND_LOG]:
                    self.attach_state_to_operators_after_snapshot(None)
                    state, metadata = self.async_snapshots.retrieve_snapshot(snapshot_id, self.local_state)

                await self.function_execution_protocol.stop()

                self.function_execution_protocol = AriaProtocol(worker_id=self.id,
                                                                peers=self.peers,
                                                                networking=self.networking,
                                                                protocol_router=self.protocol_router,
                                                                registered_operators=self.registered_operators,
                                                                topic_partitions=self.topic_partitions,
                                                                state=self.local_state,
                                                                async_snapshots=self.async_snapshots,
                                                                snapshot_metadata=metadata)

                await self.networking.send_message(DISCOVERY_HOST, DISCOVERY_PORT,
                                                   msg=(self.id, ),
                                                   msg_type=22,
                                                   serializer=Serializer.MSGPACK)
                end_time = timer()
                logging.warning(f'Worker: {self.id} | Recovered snapshot: {snapshot_id} '
                                f'| took: {round((end_time - start_time) * 1000, 4)}ms | OTHER FAILURE')
            case 21:
                # RECOVERY_OWN
                start_time = timer()
                logging.warning('RECOVERY_OWN')
                message = self.networking.decode_message(data)
                self.id, worker_operators, self.dns, self.peers, self.operator_state_backend, snapshot_id = message
                del self.peers[self.id]

                operator: Operator
                for tup in worker_operators:
                    operator, partition = tup
                    self.registered_operators[(operator.name, partition)] = operator
                    if INGRESS_TYPE == 'KAFKA':
                        self.topic_partitions.append(TopicPartition(operator.name, partition))

                metadata: dict | None = None
                if self.operator_state_backend is LocalStateBackend.DICT:
                    self.async_snapshots = AsyncSnapshotsMinio(self.id, snapshot_id=snapshot_id+1)
                    state, metadata = self.async_snapshots.retrieve_snapshot(snapshot_id)
                    self.attach_state_to_operators_after_snapshot(state)
                elif self.operator_state_backend in [LocalStateBackend.HYBRID_LOG,
                                                     LocalStateBackend.LSM,
                                                     LocalStateBackend.APPEND_LOG]:
                    self.attach_state_to_operators_after_snapshot(None)
                    self.async_snapshots = IncrementalSnapshotsMinio(self.id, snapshot_id=snapshot_id+1)
                    state, metadata = self.async_snapshots.retrieve_snapshot(snapshot_id, self.local_state)

                self.function_execution_protocol = AriaProtocol(worker_id=self.id,
                                                                peers=self.peers,
                                                                networking=self.networking,
                                                                protocol_router=self.protocol_router,
                                                                registered_operators=self.registered_operators,
                                                                topic_partitions=self.topic_partitions,
                                                                state=self.local_state,
                                                                async_snapshots=self.async_snapshots,
                                                                snapshot_metadata=metadata)
                # send that we are ready to the coordinator
                await self.networking.send_message(DISCOVERY_HOST, DISCOVERY_PORT,
                                                   msg=(self.id, ),
                                                   msg_type=22,
                                                   serializer=Serializer.MSGPACK)
                end_time = timer()
                logging.warning(f'Worker: {self.id} | Recovered snapshot: {snapshot_id} '
                                f'| took: {round((end_time - start_time) * 1000, 4)}ms | OWN FAILURE')
            case 22:
                # SYNC after recovery (Everyone is healthy)
                self.function_execution_protocol.start()
                logging.warning(f'Worker recovered and ready at : {time.time()*1000}')
            case _:
                logging.error(f"Worker Service: Non supported command message type: {message_type}")

    def attach_state_to_operators_after_snapshot(self, data):
        operator_names: set[str] = set([operator.name for operator in self.registered_operators.values()])
        minio_client: Minio = Minio(MINIO_URL, access_key=MINIO_ACCESS_KEY,
                                    secret_key=MINIO_SECRET_KEY, secure=False)
        if self.operator_state_backend is LocalStateBackend.DICT:
            self.local_state = InMemoryOperatorState(operator_names)
            self.local_state.set_data_from_snapshot(data)
        elif self.operator_state_backend is LocalStateBackend.HYBRID_LOG:
            self.local_state = IncrementalOperatorState(operator_names=operator_names,
                                                        worker_id=self.id,
                                                        minio_client=minio_client,
                                                        kv_type=KVTypes.HYBRID)
        elif self.operator_state_backend is LocalStateBackend.LSM:
            self.local_state = IncrementalOperatorState(operator_names=operator_names,
                                                        worker_id=self.id,
                                                        minio_client=minio_client,
                                                        kv_type=KVTypes.LSM)
        elif self.operator_state_backend is LocalStateBackend.APPEND_LOG:
            self.local_state = IncrementalOperatorState(operator_names=operator_names,
                                                        worker_id=self.id,
                                                        minio_client=minio_client,
                                                        kv_type=KVTypes.APPEND)
        else:
            logging.error(f"Invalid operator state backend type: {self.operator_state_backend}")
            return
        for operator in self.registered_operators.values():
            operator.attach_state_networking(self.local_state, self.networking, self.dns)

    def attach_state_to_operators(self):
        operator_names: set[str] = set([operator.name for operator in self.registered_operators.values()])
        minio_client: Minio = Minio(MINIO_URL, access_key=MINIO_ACCESS_KEY,
                                    secret_key=MINIO_SECRET_KEY, secure=False)
        if self.operator_state_backend is LocalStateBackend.DICT:
            self.async_snapshots = AsyncSnapshotsMinio(self.id)
            if PROTOCOL == Protocols.Aria:
                self.local_state = InMemoryOperatorState(operator_names)
            else:
                self.local_state = UnsafeOperatorState(operator_names)
        elif self.operator_state_backend is LocalStateBackend.HYBRID_LOG:
            self.async_snapshots = IncrementalSnapshotsMinio(self.id)
            self.local_state = IncrementalOperatorState(operator_names=operator_names,
                                                        worker_id=self.id,
                                                        minio_client=minio_client,
                                                        kv_type=KVTypes.HYBRID)
        elif self.operator_state_backend is LocalStateBackend.LSM:
            self.async_snapshots = IncrementalSnapshotsMinio(self.id)
            self.local_state = IncrementalOperatorState(operator_names=operator_names,
                                                        worker_id=self.id,
                                                        minio_client=minio_client,
                                                        kv_type=KVTypes.LSM)
        elif self.operator_state_backend is LocalStateBackend.APPEND_LOG:
            self.async_snapshots = IncrementalSnapshotsMinio(self.id)
            self.local_state = IncrementalOperatorState(operator_names=operator_names,
                                                        worker_id=self.id,
                                                        minio_client=minio_client,
                                                        kv_type=KVTypes.APPEND)
        else:
            logging.error(f"Invalid operator state backend type: {self.operator_state_backend}")
            return
        for operator in self.registered_operators.values():
            operator.attach_state_networking(self.local_state, self.networking, self.dns)

    async def handle_execution_plan(self, message):
        worker_operators, self.dns, self.peers, self.operator_state_backend = message
        del self.peers[self.id]
        operator: Operator
        for tup in worker_operators:
            operator, partition = tup
            self.registered_operators[(operator.name, partition)] = operator
            if INGRESS_TYPE == 'KAFKA':
                self.topic_partitions.append(TopicPartition(operator.name, partition))
        self.attach_state_to_operators()
        logging.warning('Trying to init protocol')
        if PROTOCOL == Protocols.Aria:
            self.function_execution_protocol = AriaProtocol(worker_id=self.id,
                                                            peers=self.peers,
                                                            networking=self.networking,
                                                            protocol_router=self.protocol_router,
                                                            registered_operators=self.registered_operators,
                                                            topic_partitions=self.topic_partitions,
                                                            state=self.local_state,
                                                            async_snapshots=self.async_snapshots)
        else:
            self.function_execution_protocol = UnsafeProtocol(worker_id=self.id,
                                                              peers=self.peers,
                                                              networking=self.networking,
                                                              protocol_router=self.protocol_router,
                                                              registered_operators=self.registered_operators,
                                                              topic_partitions=self.topic_partitions,
                                                              state=self.local_state,
                                                              async_snapshots=self.async_snapshots)

        self.function_execution_protocol.start()

        logging.warning(
            f'Registered operators: {self.registered_operators} \n'
            f'Peers: {self.peers} \n'
            f'Operator locations: {self.dns}'
        )

    async def start_tcp_service(self):
        try:
            self.router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{SERVER_PORT}",
                                                         high_read=0, high_write=0)
            self.protocol_router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{PROTOCOL_PORT}",
                                                                  high_read=0, high_write=0)
            logging.warning(
                f"Worker: {self.id} TCP Server listening at 0.0.0.0:{SERVER_PORT} "
                f"IP:{self.networking.host_name}"
            )
            while True:
                resp_adr, data = await self.router.read()
                self.aio_task_scheduler.create_task(self.worker_controller(data))
        finally:
            self.router.close()

    async def register_to_coordinator(self):
        self.id = await self.networking.send_message_request_response(
            DISCOVERY_HOST, DISCOVERY_PORT,
            msg=(self.networking.host_name, ),
            msg_type=5,
            serializer=Serializer.MSGPACK
        )
        logging.warning(f"Worker id received from coordinator: {self.id}")

    async def heartbeat_coroutine(self):
        sleep_in_seconds = HEARTBEAT_INTERVAL / 1000
        while True:
            await asyncio.sleep(sleep_in_seconds)
            await self.networking.send_message(
                DISCOVERY_HOST, DISCOVERY_PORT,
                msg=(self.id, ),
                msg_type=19,
                serializer=Serializer.MSGPACK
            )

    async def main(self):
        try:
            await self.register_to_coordinator()
            self.heartbeat_task = asyncio.shield(asyncio.create_task(self.heartbeat_coroutine()))
            await self.start_tcp_service()
        finally:
            await self.networking.close_all_connections()
            self.router.close()
            self.protocol_router.close()


if __name__ == "__main__":
    uvloop.install()
    worker = Worker()
    asyncio.run(Worker().main())
