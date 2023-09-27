import asyncio
import os
import time

from timeit import default_timer as timer

import aiozmq
import uvloop
import zmq
from minio import Minio

from styx.common.logging import logging
from styx.common.networking import NetworkingManager
from styx.common.protocols import Protocols
from styx.common.serialization import Serializer

from coordinator import Coordinator

SERVER_PORT = 8888
PROTOCOL_PORT = 8889

MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']

PROTOCOL = Protocols.Aria

SNAPSHOT_BUCKET_NAME: str = os.getenv('SNAPSHOT_BUCKET_NAME', "styx-snapshots")
SNAPSHOT_FREQUENCY_SEC = int(os.getenv('SNAPSHOT_FREQUENCY_SEC', 10))

HEARTBEAT_CHECK_INTERVAL: int = int(os.getenv('HEARTBEAT_CHECK_INTERVAL', 100))  # 100ms
HEARTBEAT_LIMIT: int = int(os.getenv('HEARTBEAT_LIMIT', 3000))  # 1000ms


class CoordinatorService(object):

    def __init__(self):
        self.networking = NetworkingManager()
        self.coordinator = Coordinator(SERVER_PORT, PROTOCOL_PORT, self.networking)
        # background task references
        self.background_tasks = set()
        self.healthy_workers: set[int] = set()
        self.worker_is_healthy: dict[int, asyncio.Event] = {}

    def create_task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

    async def tcp_service(self):
        router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{SERVER_PORT}")  # coordinator
        logging.info(f"Coordinator Server listening at 0.0.0.0:{SERVER_PORT}")
        while True:
            resp_adr, data = await router.read()
            message_type: int = self.networking.get_msg_type(data)
            match message_type:
                case 2:  # SEND_EXECUTION_GRAPH
                    message = self.networking.decode_message(data)
                    # Received execution graph from a styx client
                    self.create_task(self.coordinator.submit_stateflow_graph(message[0]))
                    logging.info("Submitted Stateflow Graph to Workers")
                case 5:  # REGISTER_WORKER
                    message = self.networking.decode_message(data)
                    # A worker registered to the coordinator
                    worker_id, send_recovery = await self.coordinator.register_worker(message[0])
                    reply = self.networking.encode_message(msg=worker_id,
                                                           msg_type=5,
                                                           serializer=Serializer.MSGPACK)
                    router.write((resp_adr, reply))
                    logging.info(f"Worker registered {message} with id {reply}")
                    if send_recovery:
                        await self.coordinator.send_operators_snapshot_offsets(worker_id)
                    self.healthy_workers.add(worker_id)
                case 18:
                    # Get snap id from worker
                    worker_id, snapshot_id, start, end = self.networking.decode_message(data)
                    self.coordinator.register_snapshot(worker_id, snapshot_id)
                    logging.warning(f'Worker: {worker_id} | '
                                    f'Completed snapshot: {snapshot_id} | '
                                    f'started at: {start} | '
                                    f'ended at: {end} | '
                                    f'took: {end - start}ms')
                case 19:
                    # HEARTBEATS
                    worker_id = self.networking.decode_message(data)[0]
                    heartbeat_rcv_time = timer()
                    logging.info(f'Heartbeat received from: {worker_id} at time: {heartbeat_rcv_time}')
                    self.coordinator.register_worker_heartbeat(worker_id, heartbeat_rcv_time)
                case 22:
                    # report ready after recovery
                    worker_id = self.networking.decode_message(data)[0]
                    self.worker_is_healthy[worker_id].set()
                    logging.info(f'ready after recovery received from: {worker_id}')
                case _:
                    # Any other message type
                    logging.error(f"COORDINATOR SERVER: Non supported message type: {message_type}")

    async def heartbeat_monitor_coroutine(self):
        interval_time = HEARTBEAT_CHECK_INTERVAL / 1000
        while True:
            await asyncio.sleep(interval_time)
            workers_to_remove = set()
            heartbeat_check_time = timer()
            for worker_id, heartbeat_rcv_time in self.coordinator.worker_heartbeats.items():
                if worker_id in self.healthy_workers and \
                        (heartbeat_check_time - heartbeat_rcv_time) * 1000 > HEARTBEAT_LIMIT:
                    logging.error(f"Did not receive heartbeat for worker: {worker_id} \n"
                                  f"Initiating automatic recovery...at time: {time.time()*1000}")
                    workers_to_remove.add(worker_id)
                    self.healthy_workers.remove(worker_id)
            if workers_to_remove:
                self.worker_is_healthy = {peer_id: asyncio.Event()
                                          for peer_id in self.coordinator.worker_heartbeats.keys()}
                await self.coordinator.remove_workers(workers_to_remove)
                await self.networking.close_all_connections()
                self.create_task(self.cluster_became_healthy_monitor())

    async def cluster_became_healthy_monitor(self):
        logging.info('waiting for cluster to be ready')
        # wait for everyone to recover
        wait_remote_proc = [event.wait()
                            for event in self.worker_is_healthy.values()]
        await asyncio.gather(*wait_remote_proc)
        # notify that everyone is ready after recovery
        tasks = [
            asyncio.ensure_future(
                self.networking.send_message(worker[0], SERVER_PORT,
                                             msg=(True, ),
                                             msg_type=22,
                                             serializer=Serializer.MSGPACK))
            for worker_id, worker in self.coordinator.workers.items()]
        await asyncio.gather(*tasks)
        logging.info('ready events sent')

    async def send_snapshot_marker(self):
        while True:
            await asyncio.sleep(SNAPSHOT_FREQUENCY_SEC)
            tasks = [
                asyncio.ensure_future(
                    self.networking.send_message(worker[0], PROTOCOL_PORT,
                                                 msg=(True,),
                                                 msg_type=99,
                                                 serializer=Serializer.MSGPACK))
                for worker_id, worker in self.coordinator.workers.items()]
            await asyncio.gather(*tasks)
            logging.warning('Snapshot marker sent')

    @staticmethod
    def init_snapshot_minio_bucket():
        minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        if not minio_client.bucket_exists(SNAPSHOT_BUCKET_NAME):
            minio_client.make_bucket(SNAPSHOT_BUCKET_NAME)

    async def main(self):
        self.init_snapshot_minio_bucket()
        self.create_task(self.heartbeat_monitor_coroutine())
        if PROTOCOL == Protocols.Unsafe or PROTOCOL == Protocols.MVCC:
            self.create_task(self.send_snapshot_marker())
        await self.tcp_service()


if __name__ == "__main__":
    uvloop.install()
    coordinator_service = CoordinatorService()
    asyncio.run(coordinator_service.main())
