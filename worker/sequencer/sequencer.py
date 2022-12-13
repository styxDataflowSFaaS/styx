from styx.common.logging import logging

from worker.run_func_payload import RunFuncPayload, SequencedItem


class Sequencer:

    def __init__(self, max_size: int = None):
        self.distributed_log: list[SequencedItem] = []
        self.current_epoch: list[SequencedItem] = []
        self.t_counter = 0
        self.worker_id = -1
        self.n_workers = -1
        self.epoch_counter = 0
        self.max_size = max_size

    def set_worker_id(self, worker_id: int):
        self.worker_id = worker_id

    def set_n_workers(self, n_workers: int):
        self.n_workers = n_workers

    def sequence(self, message: RunFuncPayload):
        t_id = self.worker_id + self.t_counter * self.n_workers
        self.t_counter += 1
        logging.info(f'Sequencing message: {message.key} with t_id: {t_id}')
        self.distributed_log.append(SequencedItem(t_id, message))

    def get_epoch(self) -> list[SequencedItem]:
        if len(self.distributed_log) > 0:
            if self.max_size is None:
                self.current_epoch = self.distributed_log
                self.distributed_log = []
            else:
                self.current_epoch = self.distributed_log[:self.max_size]
                self.distributed_log = self.distributed_log[self.max_size:]
            return self.current_epoch

    def increment_epoch(self, remote_t_counters, aborted: set[int] = None, logic_aborts_everywhere: set[int] = None):
        if aborted is not None and len(aborted) > 0:
            # needed because aborted might be from a different sequencer (part of chain)
            aborted_sequence_to_reschedule: set[SequencedItem] = {item for item in self.current_epoch
                                                                  if item.t_id in aborted
                                                                  and item.t_id not in logic_aborts_everywhere}
            distributed_log_set = set(self.distributed_log)
            self.distributed_log = sorted(distributed_log_set.union(aborted_sequence_to_reschedule))
        self.epoch_counter += 1
        if remote_t_counters:
            self.t_counter = max(*remote_t_counters, self.t_counter)
        self.current_epoch = []

    def get_aborted_sequence(self, aborted: set[int], logic_aborts_everywhere: set[int]) -> list[SequencedItem]:
        if len(aborted) > 0:
            aborted_sequence: list[SequencedItem] = sorted({item for item in self.current_epoch
                                                            if item.t_id in aborted
                                                            and item.t_id not in logic_aborts_everywhere})
            return aborted_sequence
        return []
