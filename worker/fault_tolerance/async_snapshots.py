import os
import time
import io

import zmq
from minio import Minio

from styx.common.serialization import Serializer, compressed_pickle_deserialization, compressed_pickle_serialization

from worker.fault_tolerance.base_snapshoter import BaseSnapshotter

COORDINATOR_HOST: str = os.environ['DISCOVERY_HOST']
COORDINATOR_PORT: int = int(os.environ['DISCOVERY_PORT'])
MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']
SNAPSHOT_BUCKET_NAME: str = os.getenv('SNAPSHOT_BUCKET_NAME', "styx-snapshots")


class AsyncSnapshotsMinio(BaseSnapshotter):

    def __init__(self, worker_id, snapshot_id: int = 0):
        self.worker_id = worker_id
        self.snapshot_id = snapshot_id

    def increment_snapshot_id(self, _):
        self.snapshot_id += 1

    @staticmethod
    def store_snapshot(snapshot_id: int,
                       worker_id: str,
                       data: dict,
                       message_encoder,
                       start):
        minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        bytes_file: bytes = compressed_pickle_serialization(data)
        snapshot_name: str = f"snapshot_{worker_id}_{snapshot_id}.bin"
        minio_client.put_object(SNAPSHOT_BUCKET_NAME, snapshot_name, io.BytesIO(bytes_file), len(bytes_file))
        end = time.time()*1000
        msg = message_encoder(msg=(worker_id, snapshot_id, start, end),
                              msg_type=18,
                              serializer=Serializer.PICKLE)
        sync_socket_to_coordinator = zmq.Context().socket(zmq.DEALER)
        sync_socket_to_coordinator.connect(f'tcp://{COORDINATOR_HOST}:{COORDINATOR_PORT}')
        sync_socket_to_coordinator.send(msg)
        sync_socket_to_coordinator.close()
        return True

    def retrieve_snapshot(self, snapshot_id):
        minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        snapshot_name = f"snapshot_{self.worker_id}_{snapshot_id}.bin"
        minio_object = minio_client.get_object(SNAPSHOT_BUCKET_NAME, snapshot_name)
        loaded_data = compressed_pickle_deserialization(minio_object.data)
        minio_object.close()
        minio_object.release_conn()
        return loaded_data['data'], loaded_data['metadata']
