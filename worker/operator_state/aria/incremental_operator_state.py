import os
from enum import Enum, auto

from kevo.engines import LSMTree, AppendLog, HybridLog
from kevo import MinioRemote

from minio import Minio


from styx.common.logging import logging
from styx.common.serialization import msgpack_serialization, msgpack_deserialization

from worker.operator_state.aria.base_aria_state import BaseAriaState


class KVTypes(Enum):
    LSM = auto()
    HYBRID = auto()
    APPEND = auto()


class IncrementalOperatorState(BaseAriaState):

    data: dict[str, LSMTree | HybridLog | AppendLog]

    def __init__(self,
                 operator_names: set[str],
                 worker_id: int,
                 minio_client: Minio,
                 kv_type: KVTypes = KVTypes.HYBRID):
        super().__init__(operator_names)
        self.create_folder_structure()
        self.data = {}
        if kv_type == KVTypes.LSM:
            self.data = {operator_name: LSMTree(data_dir=f'/usr/local/styx/data/{operator_name}',
                                                remote=MinioRemote(bucket=f'lsm-{operator_name}-{worker_id}',
                                                                   minio_client=minio_client)
                                                )
                         for operator_name in operator_names}
        elif kv_type == KVTypes.HYBRID:
            self.data = {operator_name: HybridLog(data_dir=f'/usr/local/styx/data/{operator_name}',
                                                  remote=MinioRemote(bucket=f'hybrid-{operator_name}-{worker_id}',
                                                                     minio_client=minio_client))
                         for operator_name in operator_names}
            logging.warning('HYBRID LOG init finished')
        elif kv_type == KVTypes.APPEND:
            self.data = {operator_name: AppendLog(data_dir=f'/usr/local/styx/data/{operator_name}',
                                                  remote=MinioRemote(bucket=f'append-{operator_name}-{worker_id}',
                                                                     minio_client=minio_client))
                         for operator_name in operator_names}
        else:
            logging.error(f'{kv_type} is not an available KV Store')

    @staticmethod
    def create_folder_structure():
        data_path = '/usr/local/styx/data'
        if not os.path.isdir(data_path):
            os.mkdir(data_path)

    async def commit_fallback_transaction(self, t_id: int):
        logging.info(f'fallback_commit_buffer : {self.fallback_commit_buffer}')
        if t_id in self.fallback_commit_buffer:
            for operator_name, kv_pairs in self.fallback_commit_buffer[t_id].items():
                for key, value in kv_pairs.items():
                    logging.info(f'committing : {key}: {value}')
                    self.data[operator_name].set(msgpack_serialization(key), msgpack_serialization(value))

    async def get(self, key, t_id: int, operator_name: str):
        self.deal_with_reads(key, t_id, operator_name)
        # if transaction wrote to this key, read from the write set
        if t_id in self.write_sets[operator_name] and key in self.write_sets[operator_name][t_id]:
            return self.write_sets[operator_name][t_id][key]
        return msgpack_deserialization(self.data[operator_name].get(msgpack_serialization(key)))

    async def get_immediate(self, key, t_id: int, operator_name: str):
        if t_id in self.fallback_commit_buffer:
            if operator_name in self.fallback_commit_buffer[t_id]:
                if key in self.fallback_commit_buffer[t_id][operator_name]:
                    return self.fallback_commit_buffer[t_id][operator_name][key]
        return msgpack_deserialization(self.data[operator_name].get(msgpack_serialization(key)))

    async def delete(self, key, operator_name: str):
        # Need to find a way to implement deletes
        pass

    async def exists(self, key, operator_name: str):
        return self.data[operator_name][msgpack_serialization(key)] is not None

    async def commit(self, aborted_from_remote: set[int]) -> set[int]:
        committed_t_ids = set()
        for operator_name in self.write_sets.keys():
            updates_to_commit = {}
            for t_id, ws in self.write_sets[operator_name].items():
                if t_id not in aborted_from_remote:
                    updates_to_commit.update(ws)
                    committed_t_ids.add(t_id)
            for key, value in updates_to_commit.items():
                self.data[operator_name].set(msgpack_serialization(key), msgpack_serialization(value))
        return committed_t_ids
