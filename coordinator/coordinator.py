import asyncio
import os
import time

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, NodeNotReadyError, TopicAlreadyExistsError

from styx.common.base_operator import BaseOperator
from styx.common.local_state_backends import LocalStateBackend
from styx.common.networking import NetworkingManager
from styx.common.stateflow_graph import StateflowGraph
from styx.common.stateflow_ingress import IngressTypes
from styx.common.logging import logging

from scheduler.round_robin import RoundRobin


class NotAStateflowGraph(Exception):
    pass


class Coordinator(object):

    def __init__(self, server_port: int, protocol_port: int, networking: NetworkingManager):
        self.networking = networking
        self.worker_counter: int = 0
        self.workers: dict[int, tuple[str, int]] = {}
        self.server_port = server_port
        self.protocol_port = protocol_port
        self.graph_submitted: bool = False

        self.worker_snapshot_ids: dict[int, int] = {}
        self.worker_heartbeats: dict[int, float] = {}
        self.dead_workers: set[int] = set()

        self.worker_assignments: dict[tuple[str, int], list[tuple[BaseOperator, int]]] = ...
        self.operator_partition_locations: dict[str, dict[str, tuple[str, int]]] = ...
        self.operator_state_backend: LocalStateBackend = ...

    async def register_worker(self, worker_ip: str) -> tuple[int, bool]:
        # a worker died before a new one registered
        id_not_ready: bool = True
        send_recovery: bool = False
        worker_id: int = -1
        while id_not_ready:
            if self.dead_workers:
                dead_worker_id: int = self.dead_workers.pop()
                self.workers[dead_worker_id] = (worker_ip, self.protocol_port)
                logging.info(f'Assigned: {dead_worker_id} to {worker_ip}')
                if self.graph_submitted:
                    self.change_operator_partition_locations(dead_worker_id, worker_ip)
                    send_recovery = True
                worker_id = dead_worker_id
                id_not_ready = False
            # a worker registers without any dead workers in the cluster
            else:
                if self.graph_submitted:
                    # sleep for 10ms waiting for heartbeat to detect
                    await asyncio.sleep(0.01)
                else:
                    self.worker_counter += 1
                    self.workers[self.worker_counter] = (worker_ip, self.protocol_port)
                    self.worker_snapshot_ids[self.worker_counter] = -1
                    worker_id = self.worker_counter
                    id_not_ready = False
        self.worker_heartbeats[worker_id] = 1_000_000.0
        return worker_id, send_recovery

    async def remove_workers(self, workers_to_remove: set[int]):
        self.dead_workers |= workers_to_remove
        await self.send_recovery_to_healthy_workers(workers_to_remove)

    def change_operator_partition_locations(self, dead_worker_id: int, worker_ip: str):
        removed_ip = self.workers[dead_worker_id][0]
        new_operator_partition_locations = {}
        for operator_name, partition_dict in self.operator_partition_locations.items():
            new_operator_partition_locations[operator_name] = {}
            for partition, worker in partition_dict.items():
                if worker[0] == removed_ip:
                    new_operator_partition_locations[operator_name][partition] = (worker_ip, self.protocol_port)
                else:
                    new_operator_partition_locations[operator_name][partition] = worker
        self.operator_partition_locations = new_operator_partition_locations

    def register_worker_heartbeat(self, worker_id: int, heartbeat_time: float):
        self.worker_heartbeats[worker_id] = heartbeat_time

    def register_snapshot(self, worker_id: int, snapshot_id: int):
        self.worker_snapshot_ids[worker_id] = snapshot_id

    def get_current_completed_snapshot_id(self):
        return min(self.worker_snapshot_ids.values())

    async def submit_stateflow_graph(self,
                                     stateflow_graph: StateflowGraph,
                                     ingress_type: IngressTypes = IngressTypes.KAFKA,
                                     scheduler_type=None):
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        scheduler = RoundRobin()
        if ingress_type == IngressTypes.KAFKA:
            self.create_kafka_ingress_topics(stateflow_graph)
        self.worker_assignments, self.operator_partition_locations, self.operator_state_backend = \
            await scheduler.schedule(self.workers, stateflow_graph, self.networking)
        self.graph_submitted = True

    async def send_operators_snapshot_offsets(self, dead_worker_id: int):
        # wait for 100ms for the resurrected worker to receive its assignment
        await asyncio.sleep(0.1)
        worker = self.workers[dead_worker_id]
        operator_partitions = self.worker_assignments[worker]
        await self.networking.send_message(worker[0], self.server_port,
                                           msg=(dead_worker_id,
                                                operator_partitions,
                                                self.operator_partition_locations,
                                                self.workers,
                                                self.operator_state_backend,
                                                self.get_current_completed_snapshot_id()),
                                           msg_type=21)
        logging.info(f'SENT RECOVER TO DEAD WORKER: {worker[0]}:{worker[1]}')

    async def send_recovery_to_healthy_workers(self, workers_to_remove: set[int]):
        # wait till all the workers have been reassigned
        if self.graph_submitted:
            while True:
                if not self.dead_workers:
                    break
                await asyncio.sleep(0.01)
            tasks = [
                asyncio.ensure_future(
                    self.networking.send_message(worker[0], self.server_port,
                                                 msg=(self.operator_partition_locations,
                                                      self.workers,
                                                      self.operator_state_backend,
                                                      self.get_current_completed_snapshot_id()),
                                                 msg_type=20))
                for worker_id, worker in self.workers.items() if worker_id not in workers_to_remove]

            await asyncio.gather(*tasks)
            logging.info('SENT RECOVER TO HEALTHY WORKERS')

    def create_kafka_ingress_topics(self, stateflow_graph: StateflowGraph):
        kafka_url: str = os.getenv('KAFKA_URL', None)
        if kafka_url is None:
            logging.error('Kafka URL not given')
        while True:
            try:
                client = KafkaAdminClient(bootstrap_servers=kafka_url)
                break
            except (NoBrokersAvailable, NodeNotReadyError):
                logging.warning(f'Kafka at {kafka_url} not ready yet, sleeping for 1 second')
                time.sleep(1)
        topics = [NewTopic(name=operator.name, num_partitions=operator.n_partitions, replication_factor=1)
                  for operator in stateflow_graph.nodes.values()] + [NewTopic(name='styx-egress',
                                                                              num_partitions=self.worker_counter,
                                                                              replication_factor=1)]
        try:
            client.create_topics(topics)
        except TopicAlreadyExistsError:
            logging.warning('Some of the Kafka topics already exists, job already submitted or rescaling')
