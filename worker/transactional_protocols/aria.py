import asyncio
import os
import concurrent.futures
import time

from timeit import default_timer as timer

from aiozmq import ZmqStream
from aiokafka import TopicPartition

from styx.common.logging import logging
from styx.common.networking import NetworkingManager
from styx.common.operator import Operator
from styx.common.serialization import Serializer, msgpack_serialization

from worker.egress.styx_kafka_egress import StyxKafkaEgress
from worker.fault_tolerance.async_snapshots import AsyncSnapshotsMinio
from worker.ingress.styx_kafka_ingress import StyxKafkaIngress
from worker.operator_state.aria.conflict_detection_types import AriaConflictDetectionType
from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.operator_state.aria.incremental_operator_state import IncrementalOperatorState
from worker.operator_state.stateless import Stateless
from worker.run_func_payload import SequencedItem, RunFuncPayload
from worker.sequencer.sequencer import Sequencer
from worker.transactional_protocols.base_protocol import BaseTransactionalProtocol
from worker.util.aio_task_scheduler import AIOTaskScheduler

CONFLICT_DETECTION_METHOD: AriaConflictDetectionType = AriaConflictDetectionType(os.getenv('CONFLICT_DETECTION_METHOD',
                                                                                           1))
# if more than 10% aborts use fallback strategy
FALLBACK_STRATEGY_PERCENTAGE: float = float(os.getenv('FALLBACK_STRATEGY_PERCENTAGE', 0.1))
# snapshot each N epochs
SNAPSHOT_FREQUENCY = int(os.getenv('SNAPSHOT_FREQUENCY', 10_000))
SEQUENCE_MAX_SIZE: int = int(os.getenv('SEQUENCE_MAX_SIZE', 100))


class AriaProtocol(BaseTransactionalProtocol):

    def __init__(self,
                 worker_id,
                 peers: dict[int, tuple[str, int]],
                 networking: NetworkingManager,
                 protocol_router: ZmqStream,
                 registered_operators: dict[tuple[str, int], Operator],
                 topic_partitions: list[TopicPartition],
                 state: InMemoryOperatorState | Stateless,
                 async_snapshots: AsyncSnapshotsMinio,
                 snapshot_metadata: dict = None):

        if snapshot_metadata is None:
            topic_partition_offsets = {(tp.topic, tp.partition): -1 for tp in topic_partitions}
            epoch_counter = 0
            t_counter = 0
            output_offset = -1
        else:
            topic_partition_offsets = snapshot_metadata["offsets"]
            epoch_counter = snapshot_metadata["epoch"]
            t_counter = snapshot_metadata["t_counter"]
            output_offset = snapshot_metadata["output_offset"]

        self.id = worker_id

        self.topic_partitions = topic_partitions
        self.networking = networking
        self.router = protocol_router

        self.local_state: InMemoryOperatorState | Stateless | IncrementalOperatorState = state
        self.aio_task_scheduler: AIOTaskScheduler = AIOTaskScheduler()
        self.async_snapshots: AsyncSnapshotsMinio = async_snapshots

        # worker_id: (host, port)
        self.peers: dict[int, tuple[str, int]] = peers
        self.topic_partition_offsets: dict[tuple[str, int], int] = topic_partition_offsets
        # worker_id: set of aborted t_ids
        self.concurrency_aborts_everywhere: set[int] = set()
        self.logic_aborts_everywhere: set[int] = set()
        self.t_ids_to_reschedule: set[int] = set()
        # t_id: (request_id, response)
        self.response_buffer: dict[int, tuple[bytes, str]] = {}
        # worker_id: size of the remote processed sequence
        self.processed_seq_size: dict[int, int] = {}
        self.t_counters: dict[int, int] = {}

        # ready_to_commit_events -> worker_id: Event that appears if the peer is ready to commit
        self.ready_to_reorder_events: dict[int, asyncio.Event] = {peer_id: asyncio.Event()
                                                                  for peer_id in self.peers.keys()}
        self.ready_to_commit_events: dict[int, asyncio.Event] = {peer_id: asyncio.Event()
                                                                 for peer_id in self.peers.keys()}
        self.fallback_done: dict[int, asyncio.Event] = {peer_id: asyncio.Event()
                                                        for peer_id in self.peers.keys()}
        self.fallback_start: dict[int, asyncio.Event] = {peer_id: asyncio.Event()
                                                         for peer_id in self.peers.keys()}
        self.proc_done: dict[int, asyncio.Event] = {peer_id: asyncio.Event()
                                                    for peer_id in self.peers.keys()}
        self.cleanup_done: dict[int, asyncio.Event] = {peer_id: asyncio.Event()
                                                       for peer_id in self.peers.keys()}

        # FALLBACK LOCKING
        # t_id: list of all the dependant transaction events
        self.waiting_on_transactions_index: dict[int, list[asyncio.Event]] = {}
        # t_id: its lock
        self.fallback_locking_event_map: dict[int, asyncio.Event] = {}

        # t_id: functions that it runs
        self.remote_function_calls: dict[int, list[RunFuncPayload]] = {}

        self.registered_operators: dict[tuple[str, int], Operator] = registered_operators

        self.sequencer = Sequencer(SEQUENCE_MAX_SIZE, t_counter=t_counter, epoch_counter=epoch_counter)
        self.sequencer.set_worker_id(self.id)
        self.sequencer.set_n_workers(len(self.peers) + 1)

        self.ingress: StyxKafkaIngress = StyxKafkaIngress(networking=self.networking,
                                                          sequencer=self.sequencer)

        self.egress: StyxKafkaEgress = StyxKafkaEgress(output_offset)

        # Primary task used for processing
        self.function_scheduler_task: asyncio.Task = ...
        self.communication_task: asyncio.Task = ...

        self.snapshot_counter = 0

    async def stop(self):
        await self.ingress.stop()
        await self.egress.stop()
        await self.aio_task_scheduler.close()
        self.function_scheduler_task.cancel()
        self.communication_task.cancel()
        try:
            await self.function_scheduler_task
            await self.communication_task
        except asyncio.CancelledError:
            logging.warning("Protocol coroutines stopped")
        logging.warning("Aria protocol stopped")

    def start(self):
        self.function_scheduler_task = asyncio.create_task(self.function_scheduler())
        self.communication_task = asyncio.create_task(self.communication_protocol())
        logging.warning("Aria protocol started")

    async def run_function(
            self,
            t_id: int,
            payload: RunFuncPayload,
            internal_msg: bool = False,
            fallback_mode: bool = False
    ) -> bool:
        success: bool = True
        operator_partition = self.registered_operators[(payload.operator_name, payload.partition)]
        self.topic_partition_offsets[(payload.operator_name, payload.partition)] = max(payload.kafka_offset,
                                                                                       self.topic_partition_offsets[
                                                                                           (payload.operator_name,
                                                                                            payload.partition)])
        response = await operator_partition.run_function(
            payload.key,
            t_id,
            payload.request_id,
            payload.timestamp,
            payload.function_name,
            payload.ack_payload,
            fallback_mode,
            payload.params
        )
        # If exception we need to add it to the application logic aborts
        if isinstance(response, Exception):
            success = False
            self.logic_aborts_everywhere.add(t_id)
        # If request response send the response
        if payload.response_socket is not None:
            self.router.write(
                (payload.response_socket, self.networking.encode_message(
                    msg=response,
                    msg_type=1,
                    serializer=Serializer.MSGPACK
                ))
            )
        # If we have a response, and it's not part of the chain send it to kafka
        # elif response is not None and not internal_msg:
        elif response is not None:
            # If Exception transform it to string for Kafka
            if isinstance(response, Exception):
                kafka_response = str(response)
            else:
                kafka_response = response
            # If fallback add it to the fallback replies else to the response buffer
            self.response_buffer[t_id] = (payload.request_id, kafka_response)
        return success

    def take_snapshot(self, pool: concurrent.futures.ProcessPoolExecutor):
        if self.sequencer.epoch_counter % SNAPSHOT_FREQUENCY == 0 and self.sequencer.epoch_counter != 0:
            if InMemoryOperatorState.__name__ == self.local_state.__class__.__name__:
                loop = asyncio.get_running_loop()
                start = time.time() * 1000
                data = {"data":  self.local_state.data,
                        "metadata": {"offsets": self.topic_partition_offsets,
                                     "epoch": self.sequencer.epoch_counter,
                                     "t_counter": self.sequencer.t_counter,
                                     "output_offset": self.egress.output_offset}}
                loop.run_in_executor(pool, self.async_snapshots.store_snapshot, self.async_snapshots.snapshot_id,
                                     self.id, data, self.networking.encode_message,
                                     start).add_done_callback(self.async_snapshots.increment_snapshot_id)
            elif IncrementalOperatorState.__name__ == self.local_state.__class__.__name__:
                self.local_state: IncrementalOperatorState
                loop = asyncio.get_running_loop()
                start = time.time() * 1000
                # store state
                for operator_name, kv_store in self.local_state.data.items():
                    kv_store.snapshot(self.async_snapshots.snapshot_id)
                metadata = {"offsets": self.topic_partition_offsets,
                            "epoch": self.sequencer.epoch_counter,
                            "t_counter": self.sequencer.t_counter,
                            "output_offset": self.egress.output_offset}
                loop.run_in_executor(pool, self.async_snapshots.store_snapshot,
                                     self.async_snapshots.snapshot_id,
                                     self.id, metadata, self.networking.encode_message,
                                     start).add_done_callback(self.async_snapshots.increment_snapshot_id)
            else:
                logging.warning("Snapshot currently supported only for in-memory and incremental operator state")

    async def communication_protocol(self):
        self.ingress.start(self.topic_partitions, self.topic_partition_offsets)
        logging.warning('Ingress started')
        await self.egress.start(self.id)
        logging.warning('Egress started')
        while True:
            resp_adr, data = await self.router.read()
            self.aio_task_scheduler.create_task(self.protocol_tcp_controller(data, resp_adr))

    async def protocol_tcp_controller(self, data: bytes, resp_adr):
        message_type: int = self.networking.get_msg_type(data)
        match message_type:
            case 0:
                logging.info('CALLED RUN FUN FROM PEER')
                message = self.networking.decode_message(data)
                t_id, request_id, operator_name, function_name, key, partition, timestamp, params, ack = message
                payload = RunFuncPayload(request_id=request_id, key=key, timestamp=timestamp,
                                         operator_name=operator_name, partition=partition,
                                         function_name=function_name, params=params, ack_payload=ack)
                if t_id in self.remote_function_calls:
                    self.remote_function_calls[t_id].append(payload)
                else:
                    self.remote_function_calls[t_id] = [payload]
                await self.run_function(
                    t_id,
                    payload,
                    internal_msg=True
                )
            case 1:
                logging.info('CALLED RUN FUN RQ RS FROM PEER')
                message = self.networking.decode_message(data)
                t_id, request_id, operator_name, function_name, key, partition, timestamp, params = message
                payload = RunFuncPayload(request_id=request_id, key=key, timestamp=timestamp,
                                         operator_name=operator_name, partition=partition,
                                         function_name=function_name, params=params, response_socket=resp_adr)
                await self.run_function(
                    t_id,
                    payload,
                    internal_msg=True
                )
            case 7:
                message = self.networking.decode_message(data)
                remote_worker_id, aborted, remote_t_counter, processed_seq_size = message
                self.ready_to_commit_events[remote_worker_id].set()
                self.concurrency_aborts_everywhere |= aborted
                self.processed_seq_size[remote_worker_id] = processed_seq_size
                self.t_counters[remote_worker_id] = remote_t_counter
            case 12:
                message = self.networking.decode_message(data)
                remote_worker_id = message[0]
                self.fallback_done[remote_worker_id].set()
            case 13:
                message = self.networking.decode_message(data)
                remote_worker_id = message[0]
                self.fallback_start[remote_worker_id].set()
            case 9:
                message = self.networking.decode_message(data)
                remote_worker_id, remote_logic_aborts = message
                self.proc_done[remote_worker_id].set()
                self.logic_aborts_everywhere |= remote_logic_aborts
            case 3:
                message = self.networking.decode_message(data)
                ack_id, fraction_str, exception_str = message
                if fraction_str != '-1':
                    self.networking.add_ack_fraction_str(ack_id, fraction_str)
                    # logging.warning(f'ACK received for {ack_id} part: {fraction_str}')
                else:
                    # logging.warning(f'ABORT ACK received for {ack_id}')
                    self.networking.abort_chain(ack_id, exception_str)
                    self.logic_aborts_everywhere.add(ack_id)
            case 14:
                message = self.networking.decode_message(data)
                # fallback phase
                # here we handle the logic to unlock locks held by the provided distributed transaction
                t_id, success = message
                if success:
                    # commit changes
                    await self.local_state.commit_fallback_transaction(t_id)
                # unlock
                # logging.warning(f'Unlocking: {t_id}')
                self.unlock_tid(t_id)
            case 17:
                message = self.networking.decode_message(data)
                remote_worker_id, remote_read_reservation, remote_write_set, remote_read_set = message
                self.ready_to_reorder_events[remote_worker_id].set()
                self.local_state.merge_rr(remote_read_reservation)
                self.local_state.merge_ws(remote_write_set)
                self.local_state.merge_rs(remote_read_set)
            case 23:
                message = self.networking.decode_message(data)
                remote_worker_id = message[0]
                self.cleanup_done[remote_worker_id].set()
            case _:
                logging.error(f"Aria protocol: Non supported command message type: {message_type}")

    async def function_scheduler(self):
        # await self.started.wait()
        logging.warning('STARTED function scheduler')
        with concurrent.futures.ProcessPoolExecutor(1) as pool:
            while True:
                # need to sleep to allow the kafka consumer coroutine to read data
                await asyncio.sleep(0)
                async with self.sequencer.lock:
                    # GET SEQUENCE
                    sequence: list[SequencedItem] = self.sequencer.get_epoch()
                    if sequence:
                        # logging.warning(f'Sequence tids: {[pld.t_id for pld in sequence]}')
                        logging.info(f'Epoch: {self.sequencer.epoch_counter} starts')
                        # Run all the epochs functions concurrently
                        epoch_start = timer()
                        logging.info('Running functions...')
                        # async with self.snapshot_state_lock:
                        run_function_tasks = [self.run_function(sequenced_item.t_id, sequenced_item.payload)
                                              for sequenced_item in sequence]
                        await asyncio.gather(*run_function_tasks)
                        # function_running_done = timer()
                        # self.function_running_time += function_running_done - epoch_start
                        # Wait for chains to finish
                        logging.info('Waiting on chained functions...')
                        chain_acks = [ack.wait()
                                      for ack in self.networking.waited_ack_events.values()]
                        await asyncio.gather(*chain_acks)
                        # function_chains_done = timer()
                        # self.chain_completion_time += function_chains_done - function_running_done
                        # wait for all peers to be done processing (needed to know the aborts)
                        await self.wait_function_running_done()
                        # sync_time = timer()
                        # self.sync_time += sync_time - function_chains_done
                        logging.info(f'logic_aborts_everywhere: {self.logic_aborts_everywhere}')
                        # HERE WE KNOW ALL THE LOGIC ABORTS
                        # removing the global logic abort transactions from the commit phase
                        self.local_state.remove_aborted_from_rw_sets(self.logic_aborts_everywhere)
                        # Check for local state conflicts
                        logging.info('Checking conflicts...')
                        concurrency_aborts: set[int] = set()
                        if CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.DEFAULT_SERIALIZABLE:
                            concurrency_aborts: set[int] = self.local_state.check_conflicts()
                        elif CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.DETERMINISTIC_REORDERING:
                            await self.wait_read_write_set_communication()
                            concurrency_aborts: set[int] = self.local_state.check_conflicts_deterministic_reordering()
                        elif CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.SNAPSHOT_ISOLATION:
                            concurrency_aborts: set[int] = self.local_state.check_conflicts_snapshot_isolation()
                        elif CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.SERIALIZABLE_REORDER_ON_IN_DEGREE:
                            await self.wait_read_write_set_communication()
                            concurrency_aborts: set[int] = \
                                self.local_state.check_conflicts_serial_reorder_on_in_degree()
                        elif CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.SNAPSHOT_REORDER_ON_IN_DEGREE:
                            concurrency_aborts: set[int] = \
                                self.local_state.check_conflicts_snapshot_isolation_reorder_on_in_degree()
                        else:
                            logging.error('This conflict detection method number is not a valid number')
                        self.concurrency_aborts_everywhere |= concurrency_aborts
                        # conflict_resolution_time = timer()
                        # self.conflict_resolution_time += conflict_resolution_time - sync_time
                        # Notify peers that we are ready to commit
                        # TODO CHECK IF WE CAN ESCAPE THIS THIS(WE DO NOT NEED THAT IF WE HAVE COMMUNICATED THE RW SETS)
                        logging.info('Notify peers...')
                        await self.send_commit_to_peers(concurrency_aborts, len(sequence))
                        # HERE WE KNOW ALL THE CONCURRENCY ABORTS
                        # Wait for remote to be ready to commit
                        logging.info('Waiting on remote commits...')
                        await self.wait_commit()
                        # sync_time = timer()
                        # self.sync_time += sync_time - conflict_resolution_time
                        # Gather the remote concurrency aborts
                        # Commit the local while taking into account the aborts from remote
                        logging.info('Starting commit!')
                        await self.local_state.commit(self.concurrency_aborts_everywhere)
                        logging.info('Sequence committed!')
                        # commit_done = timer()
                        # self.commit_time += commit_done - sync_time

                        self.t_ids_to_reschedule = self.concurrency_aborts_everywhere - self.logic_aborts_everywhere

                        self.send_responses(
                            self.t_ids_to_reschedule,
                            self.response_buffer.copy()
                        )
                        total_processed_functions: int = sum(self.processed_seq_size.values()) + len(sequence)
                        abort_rate: float = len(self.concurrency_aborts_everywhere) / total_processed_functions

                        if abort_rate > FALLBACK_STRATEGY_PERCENTAGE:
                            # Run Calvin
                            logging.info(
                                f'Epoch: {self.sequencer.epoch_counter} '
                                f'Abort percentage: {int(abort_rate * 100)}% initiating fallback strategy...\n'
                                # f'reads: {self.local_state.reads}\n'
                                # f'writes: {self.local_state.writes}\n'
                                # f'c_aborts: {self.concurrency_aborts_everywhere - self.logic_aborts_everywhere}'
                            )
                            # start_fallback = timer()
                            # logging.warning(f'Logic abort ti_ds: {logic_aborts_everywhere}')
                            await self.run_fallback_strategy()
                            self.concurrency_aborts_everywhere = set()
                            self.t_ids_to_reschedule = set()
                            # end_fallback = timer()
                            # self.fallback_time += end_fallback - start_fallback
                        # Cleanup
                        # start_seq_incr = timer()
                        self.sequencer.increment_epoch(
                            list(self.t_counters.values()),
                            self.t_ids_to_reschedule
                        )
                        # end_seq_incr = timer()
                        # self.sequencing_time += end_seq_incr - start_seq_incr
                        self.t_counters = {}
                        # Re-sequence the aborted transactions due to concurrency
                        epoch_end = timer()

                        logging.info(
                            f'Epoch: {self.sequencer.epoch_counter - 1} done in '
                            f'{round((epoch_end - epoch_start) * 1000, 4)}ms '
                            f'processed: {len(run_function_tasks)} functions '
                            f'initiated {len(chain_acks)} chains '
                            f'global logic aborts: {len(self.logic_aborts_everywhere)} '
                            f'concurrency aborts for next epoch: {len(self.concurrency_aborts_everywhere)} '
                            f'abort rate: {abort_rate}'
                        )
                        # logging.warning(f'Epoch: {self.sequencer.epoch_counter - 1} done in '
                        #                 f'{round((epoch_end - epoch_start) * 1000, 4)}ms '
                        #                 f'function_running_time: {self.function_running_time}\n'
                        #                 f'chain_completion_time: {self.chain_completion_time}\n'
                        #                 f'serialization_time: {self.serialization_time}\n'
                        #                 f'sequencing_time: {self.sequencing_time}\n'
                        #                 f'conflict_resolution_time: {self.conflict_resolution_time}\n'
                        #                 f'commit_time: {self.commit_time}\n'
                        #                 f'sync_time: {self.sync_time}\n'
                        #                 f'snapshot_time: {self.snapshot_time}\n'
                        #                 f'fallback_time: {self.fallback_time}\n')
                        self.cleanup_after_epoch()
                        # start_sn = timer()
                        self.take_snapshot(pool)
                        await self.wait_cleanup_done()
                        # end_sn = timer()
                        # self.snapshot_time += end_sn - start_sn
                    elif self.remote_wants_to_proceed():
                        # wait for all peers to be done processing (needed to know the aborts)
                        await self.handle_nothing_to_commit_case()
                        # start_sn = timer()
                        self.take_snapshot(pool)
                        await self.wait_cleanup_done()
                        # end_sn = timer()
                        # self.snapshot_time += end_sn - start_sn

    async def handle_nothing_to_commit_case(self):
        logging.info(f'Epoch: {self.sequencer.epoch_counter} starts')
        epoch_start = timer()
        await self.wait_function_running_done()
        # end_func_r = timer()
        # self.function_running_time += end_func_r - epoch_start
        logging.info(f'logic_aborts_everywhere: {self.logic_aborts_everywhere}')
        self.local_state.remove_aborted_from_rw_sets(self.logic_aborts_everywhere)
        concurrency_aborts: set[int] = set()
        if CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.DEFAULT_SERIALIZABLE:
            concurrency_aborts: set[int] = self.local_state.check_conflicts()
        elif CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.DETERMINISTIC_REORDERING:
            await self.wait_read_write_set_communication()
            concurrency_aborts: set[int] = self.local_state.check_conflicts_deterministic_reordering()
        elif CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.SNAPSHOT_ISOLATION:
            concurrency_aborts: set[int] = self.local_state.check_conflicts_snapshot_isolation()
        elif CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.SERIALIZABLE_REORDER_ON_IN_DEGREE:
            await self.wait_read_write_set_communication()
            concurrency_aborts: set[int] = \
                self.local_state.check_conflicts_serial_reorder_on_in_degree()
        elif CONFLICT_DETECTION_METHOD is AriaConflictDetectionType.SNAPSHOT_REORDER_ON_IN_DEGREE:
            concurrency_aborts: set[int] = \
                self.local_state.check_conflicts_snapshot_isolation_reorder_on_in_degree()
        else:
            logging.error('This conflict detection method number is not a valid number')
        self.concurrency_aborts_everywhere |= concurrency_aborts
        # conflict_resolution_time = timer()
        # self.conflict_resolution_time += conflict_resolution_time - end_func_r
        logging.info(f'local_aborted: {concurrency_aborts}')
        await self.send_commit_to_peers(concurrency_aborts, 0)
        # Wait for remote to be ready to commit
        logging.info('Waiting on remote commits...')
        await self.wait_commit()
        # sync_done = timer()
        # self.sync_time += sync_done - conflict_resolution_time

        logging.info('Starting commit!')
        await self.local_state.commit(self.concurrency_aborts_everywhere)
        # commit_time = timer()
        # self.commit_time += commit_time - sync_done

        self.t_ids_to_reschedule = self.concurrency_aborts_everywhere - self.logic_aborts_everywhere

        self.send_responses(
            self.t_ids_to_reschedule,
            self.response_buffer.copy()
        )

        total_processed_functions: int = sum(self.processed_seq_size.values())
        abort_rate: float = len(self.concurrency_aborts_everywhere) / total_processed_functions
        if abort_rate > FALLBACK_STRATEGY_PERCENTAGE:
            # fallback_start = timer()
            logging.info(
                f'FB Epoch: {self.sequencer.epoch_counter} '
                f'Abort percentage: {int(abort_rate * 100)}% initiating fallback strategy...'
            )

            await self.run_fallback_strategy()
            self.concurrency_aborts_everywhere = set()
            self.t_ids_to_reschedule = set()
            # fallback_end = timer()
            # self.fallback_time += fallback_end - fallback_start

        # start_seq_inc = timer()
        self.sequencer.increment_epoch(list(self.t_counters.values()),
                                       self.t_ids_to_reschedule)
        # end_seq_inc = timer()
        # self.sequencing_time += end_seq_inc - start_seq_inc
        self.t_counters = {}
        epoch_end = timer()
        logging.info(
            f'Epoch: {self.sequencer.epoch_counter - 1} done in '
            f'{round((epoch_end - epoch_start) * 1000, 4)}ms '
            f'processed 0 functions directly '
            f'global logic aborts: {len(self.logic_aborts_everywhere)} '
            f'concurrency aborts for next epoch: {len(self.concurrency_aborts_everywhere)}'
        )
        # logging.warning(f'Epoch: {self.sequencer.epoch_counter - 1} done in '
        #                 f'{round((epoch_end - epoch_start) * 1000, 4)}ms '
        #                 f'function_running_time: {self.function_running_time}\n'
        #                 f'chain_completion_time: {self.chain_completion_time}\n'
        #                 f'serialization_time: {self.serialization_time}\n'
        #                 f'sequencing_time: {self.sequencing_time}\n'
        #                 f'conflict_resolution_time: {self.conflict_resolution_time}\n'
        #                 f'commit_time: {self.commit_time}\n'
        #                 f'sync_time: {self.sync_time}\n'
        #                 f'snapshot_time: {self.snapshot_time}\n'
        #                 f'fallback_time: {self.fallback_time}\n')
        self.cleanup_after_epoch()

    def cleanup_after_epoch(self):
        self.remote_function_calls = {}
        self.concurrency_aborts_everywhere = set()
        self.logic_aborts_everywhere = set()
        self.t_ids_to_reschedule = set()
        self.networking.cleanup_after_epoch()
        self.local_state.cleanup()
        self.response_buffer = {}

    def remote_wants_to_proceed(self) -> bool:
        for event in self.proc_done.values():
            if event.is_set():
                return True
        return False

    async def run_fallback_function(
            self,
            t_id: int,
            payload: RunFuncPayload,
            internal: bool = False
    ):
        # Wait for all transactions that this transaction depends on to finish
        if t_id in self.waiting_on_transactions_index:
            waiting_on_transactions_tasks = [dependency.wait()
                                             for dependency in self.waiting_on_transactions_index[t_id]]
            await asyncio.gather(*waiting_on_transactions_tasks)

        # Run transaction
        success = await self.run_function(t_id, payload, internal_msg=internal, fallback_mode=True)
        # logging.warning(f'Finished fallback function: {t_id}:{internal}')
        if not internal:
            # Run local "remote" function calls of the same transaction
            # local_t_functions: list[RunFuncPayload] = self.remote_function_calls.get(t_id, [])
            # logging.warning(f'local_t_functions of tid: {t_id} -> {len(local_t_functions)}')
            # if local_t_functions is not None:
            #     # logging.warning(f'internal_remote_function_call t_id: {t_id}:{payload.key}')
            #     local_t_function_tasks = []
            #     for pld in local_t_functions:
            #         local_t_function_tasks.append(self.run_function(t_id, pld, internal_msg=True, fallback_mode=True))
            #     await asyncio.gather(*local_t_function_tasks)

            # if root of chain
            if t_id in self.networking.waited_ack_events:
                # wait on ack of parts
                # logging.warning(f'waiting for ack of {t_id} with fraction: {self.networking.ack_fraction[t_id]}')
                await self.networking.waited_ack_events[t_id].wait()
            # logging.warning(f'Net unlk: {t_id}')
            if t_id in self.networking.aborted_events or not success:
                await self.send_fallback_unlock_to_peers(t_id, success=False)
            else:
                await self.local_state.commit_fallback_transaction(t_id)
                await self.send_fallback_unlock_to_peers(t_id, success=True)
            # Release the locks for local
            # logging.warning(f'Unlocking: {t_id}')
            self.unlock_tid(t_id)
            # if network failed send the buffered exception else the response buffer no need for this complexity
            if t_id in self.networking.aborted_events:
                await self.egress.send(key=payload.request_id,
                                       value=msgpack_serialization(self.networking.aborted_events[t_id]))
            else:
                await self.egress.send(key=payload.request_id,
                                       value=msgpack_serialization(self.response_buffer[t_id][1]))

    def fallback_locking_mechanism(self):
        merged_rw_reservations: dict[str, dict[any, list[int]]] = self.local_state.merge_rw_reservations_fallback(
            self.t_ids_to_reschedule)
        for operator, reservations in merged_rw_reservations.items():
            for transactions in reservations.values():
                for idx, t_id in enumerate(transactions):
                    if idx > 0:
                        self.set_transaction_fallback_locks(t_id, transactions[:idx])

    def set_transaction_fallback_locks(self, t_id: int, dep_transactions: list[int]):
        for dep_t in dep_transactions:
            if dep_t not in self.fallback_locking_event_map:
                self.fallback_locking_event_map[dep_t] = asyncio.Event()
            if t_id in self.waiting_on_transactions_index:
                self.waiting_on_transactions_index[t_id].append(self.fallback_locking_event_map[dep_t])
            else:
                self.waiting_on_transactions_index[t_id] = [self.fallback_locking_event_map[dep_t]]

    async def run_fallback_strategy(self):
        logging.info('Starting fallback strategy...')

        self.fallback_locking_mechanism()

        aborted_sequence: list[SequencedItem] = self.sequencer.get_aborted_sequence(self.t_ids_to_reschedule)
        logging.info(f'aborted_sequence: {aborted_sequence}')
        fallback_tasks = []
        for sequenced_item in aborted_sequence:
            # current worker is the root of the chain
            fallback_tasks.append(
                self.run_fallback_function(
                    sequenced_item.t_id,
                    sequenced_item.payload
                )
            )

        aborted_sequence_t_ids: set[int] = {item.t_id for item in aborted_sequence}
        self.networking.reset_ack_for_fallback(ack_ids=aborted_sequence_t_ids)
        # logging.warning(f'Networking acks: {self.networking.ack_fraction}')
        # rmfc = []
        # logging.warning(f'origin aborted_sequence_t_ids: {aborted_sequence_t_ids}')
        for t_id, payloads in self.remote_function_calls.items():
            # if t_id not in aborted_sequence_t_ids \
            #         and t_id not in self.logic_aborts_everywhere \
            #         and t_id in self.concurrency_aborts_everywhere:
            if t_id in self.t_ids_to_reschedule:
                # part of the chain since it came from remote
                # rmfc.append(t_id)
                for payload in payloads:
                    fallback_tasks.append(
                        self.run_fallback_function(
                            t_id,
                            payload,
                            internal=True
                        )
                    )
        # logging.warning(f'remote aborted_sequence_t_ids: {rmfc}')
        await self.wait_fallback_start_sync()
        await asyncio.gather(*fallback_tasks)
        logging.info(
            f'Epoch: {self.sequencer.epoch_counter} '
            f'Fallback strategy done waiting for peers'
        )
        await self.wait_fallback_done_sync()

    def unlock_tid(self, t_id_to_unlock: int):
        if t_id_to_unlock in self.fallback_locking_event_map:
            self.fallback_locking_event_map[t_id_to_unlock].set()

    def send_responses(
            self,
            t_ids_to_reschedule: set[int],
            response_buffer: dict[int, tuple[bytes, str]]
    ):
        for t_id, response in response_buffer.items():
            if t_id not in t_ids_to_reschedule:
                if t_id in self.networking.aborted_events:
                    self.aio_task_scheduler.create_task(self.egress.send(key=response[0],
                                                                         value=msgpack_serialization(
                                                          self.networking.aborted_events[t_id])))
                else:
                    self.aio_task_scheduler.create_task(self.egress.send(key=response[0],
                                                                         value=msgpack_serialization(response[1])))

    async def wait_fallback_done_sync(self):
        await self.send_sync_to_peers(12)  # FALLBACK_DONE
        wait_remote_fallback = [event.wait()
                                for event in self.fallback_done.values()]
        await asyncio.gather(*wait_remote_fallback)
        [event.clear() for event in self.fallback_done.values()]

    async def wait_fallback_start_sync(self):
        await self.send_sync_to_peers(13)  # FALLBACK_START
        wait_remote_fallback = [event.wait()
                                for event in self.fallback_start.values()]
        await asyncio.gather(*wait_remote_fallback)
        [event.clear() for event in self.fallback_start.values()]

    async def wait_function_running_done(self):
        self.aio_task_scheduler.create_task(self.send_sync_to_peers(9, send_logic_aborts=True))  # PROC_DONE
        wait_remote_proc = [event.wait()
                            for event in self.proc_done.values()]
        await asyncio.gather(*wait_remote_proc)
        [event.clear() for event in self.proc_done.values()]

    async def wait_cleanup_done(self):
        self.aio_task_scheduler.create_task(self.send_sync_to_peers(23))  # CLEANUP_DONE
        wait_remote_cleanup = [event.wait()
                               for event in self.cleanup_done.values()]
        await asyncio.gather(*wait_remote_cleanup)
        [event.clear() for event in self.cleanup_done.values()]

    async def wait_read_write_set_communication(self):
        await self.send_rr_ws_to_peers()
        exchange_rw_sets = [event.wait()
                            for event in self.ready_to_reorder_events.values()]
        await asyncio.gather(*exchange_rw_sets)
        [event.clear() for event in self.ready_to_reorder_events.values()]

    async def wait_commit(self):
        wait_remote_commits_task = [event.wait()
                                    for event in self.ready_to_commit_events.values()]
        await asyncio.gather(*wait_remote_commits_task)
        [event.clear() for event in self.ready_to_commit_events.values()]

    async def send_commit_to_peers(self, aborted: set[int], processed_seq_size: int):
        tasks = [self.networking.send_message(
            url[0],
            url[1],
            msg=(self.id,
                 aborted,
                 self.sequencer.t_counter,
                 processed_seq_size),
            msg_type=7,
            serializer=Serializer.PICKLE
        )
            for worker_id, url in self.peers.items()]
        await asyncio.gather(*tasks)

    async def send_rr_ws_to_peers(self):
        tasks = [self.networking.send_message(
            url[0],
            url[1],
            msg=(self.id,
                 self.local_state.reads,
                 self.local_state.write_sets,
                 self.local_state.read_sets),
            msg_type=17,
            serializer=Serializer.PICKLE
        )
            for worker_id, url in self.peers.items()]
        await asyncio.gather(*tasks)

    async def send_fallback_unlock_to_peers(self, t_id: int, success: bool):
        tasks = [self.networking.send_message(
            url[0],
            url[1],
            msg=(t_id, success),
            msg_type=14,
            serializer=Serializer.MSGPACK
        )
            for worker_id, url in self.peers.items()]
        await asyncio.gather(*tasks)

    async def send_sync_to_peers(self, phase: int, send_logic_aborts: bool = False):
        if send_logic_aborts:
            # share this worker's knowledge of the logic aborts
            msg = (self.id, self.logic_aborts_everywhere)
            serializer = Serializer.PICKLE
        else:
            msg = (self.id, )
            serializer = Serializer.MSGPACK
        tasks = [self.networking.send_message(
            url[0],
            url[1],
            msg=msg,
            msg_type=phase,
            serializer=serializer
        )
            for worker_id, url in self.peers.items()]
        await asyncio.gather(*tasks)
