from typing import Any

from styx.common.logging import logging

from worker.operator_state.aria.base_aria_state import BaseAriaState


class InMemoryOperatorState(BaseAriaState):

    data: dict[str, dict[Any, Any]]

    def __init__(self, operator_names: set[str]):
        super().__init__(operator_names)
        self.data = {}
        for operator_name in operator_names:
            self.data[operator_name] = {}

    def set_data_from_snapshot(self, data: dict[str, dict[Any, Any]]):
        self.data = data

    async def commit_fallback_transaction(self, t_id: int):
        logging.info(f'fallback_commit_buffer : {self.fallback_commit_buffer}')
        if t_id in self.fallback_commit_buffer:
            for operator_name, kv_pairs in self.fallback_commit_buffer[t_id].items():
                for key, value in kv_pairs.items():
                    logging.info(f'committing : {key}: {value}')
                    self.data[operator_name][key] = value

    def get_all(self, t_id: int, operator_name: str):
        [self.deal_with_reads(key, t_id, operator_name) for key in self.data[operator_name].keys()]
        return self.data[operator_name]

    async def get(self, key, t_id: int, operator_name: str) -> Any:
        self.deal_with_reads(key, t_id, operator_name)
        # if transaction wrote to this key, read from the write set
        if t_id in self.write_sets[operator_name] and key in self.write_sets[operator_name][t_id]:
            return self.write_sets[operator_name][t_id][key]
        return self.data[operator_name].get(key)

    async def get_immediate(self, key, t_id: int, operator_name: str):
        if t_id in self.fallback_commit_buffer:
            if operator_name in self.fallback_commit_buffer[t_id]:
                if key in self.fallback_commit_buffer[t_id][operator_name]:
                    return self.fallback_commit_buffer[t_id][operator_name][key]
        return self.data[operator_name].get(key)

    async def delete(self, key, operator_name: str):
        # Need to find a way to implement deletes
        pass

    async def exists(self, key, operator_name: str):
        return True if key in self.data[operator_name] else False

    async def commit(self, aborted_from_remote: set[int]) -> set[int]:
        committed_t_ids = set()
        for operator_name in self.write_sets.keys():
            updates_to_commit = {}
            for t_id, ws in self.write_sets[operator_name].items():
                if t_id not in aborted_from_remote:
                    updates_to_commit.update(ws)
                    committed_t_ids.add(t_id)
            self.data[operator_name].update(updates_to_commit)
        return committed_t_ids
