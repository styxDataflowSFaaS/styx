from abc import abstractmethod
from typing import Any

from styx.common.base_state import BaseOperatorState

from worker.operator_state.aria.conflict_detection_graph_utils import get_start_order_serialization_graph, \
    check_conflict_on_start_order_serialization_graph, get_bc_graph, check_conflicts_on_bc_graph


class BaseAriaState(BaseOperatorState):
    # read write sets
    # operator_name: {t_id: set(keys)}
    read_sets: dict[str, dict[int, set[Any]]]
    remote_read_sets: dict[str, dict[int, set[Any]]]
    # operator_name: {t_id: {key: value}}
    write_sets: dict[str, dict[int, dict[Any, Any]]]
    remote_write_sets: dict[str, dict[int, dict[Any, Any]]]
    # the reads and writes with the lowest t_id
    # operator_name: {key: t_id}
    writes: dict[str, dict[Any, list[int]]]
    # operator_name: {key: t_id}
    reads: dict[str, dict[Any, list[int]]]
    remote_reads: dict[str, dict[Any, list[int]]]
    # Calvin snapshot things
    # tid: {operator_name: {key, value}}
    fallback_commit_buffer: dict[int, dict[str, dict[Any, Any]]]

    def __init__(self, operator_names: set[str]):
        super().__init__(operator_names)
        self.cleanup()

    async def put(self, key, value, t_id: int, operator_name: str):
        if t_id in self.write_sets[operator_name]:
            self.write_sets[operator_name][t_id][key] = value
        else:
            self.write_sets[operator_name][t_id] = {key: value}
        if key in self.writes[operator_name]:
            self.writes[operator_name][key].append(t_id)
        else:
            self.writes[operator_name][key] = [t_id]

    async def put_immediate(self, key, value, t_id: int, operator_name: str):
        if t_id in self.fallback_commit_buffer:
            if operator_name in self.fallback_commit_buffer[t_id]:
                self.fallback_commit_buffer[t_id][operator_name][key] = value
            else:
                self.fallback_commit_buffer[t_id][operator_name] = {key: value}
        else:
            self.fallback_commit_buffer[t_id] = {operator_name: {key: value}}

    @abstractmethod
    async def commit_fallback_transaction(self, t_id: int):
        raise NotImplementedError

    @abstractmethod
    async def get(self, key, t_id: int, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def get_immediate(self, key, t_id: int, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def delete(self, key, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def exists(self, key, operator_name: str):
        raise NotImplementedError

    @abstractmethod
    async def commit(self, aborted_from_remote: set[int]) -> set[int]:
        raise NotImplementedError

    def deal_with_reads(self, key, t_id: int, operator_name: str):
        if key in self.reads[operator_name]:
            self.reads[operator_name][key].append(t_id)
        else:
            self.reads[operator_name][key] = [t_id]
        if t_id in self.read_sets[operator_name]:
            self.read_sets[operator_name][t_id].add(key)
        else:
            self.read_sets[operator_name][t_id] = {key}

    @staticmethod
    def has_conflicts(t_id: int, keys: set[Any], reservations: dict[Any, int]):
        for key in keys:
            if key in reservations and reservations[key] < t_id:
                return True
        return False

    @staticmethod
    def min_rw_reservations(reservations: dict[str, dict[Any, list[int]]]) -> dict[str, dict[Any, int]]:
        new__reservations = {}
        for operator_name, reservation in reservations.items():
            new__reservations[operator_name] = {key: min(t_ids) for key, t_ids in reservation.items() if t_ids}
        return new__reservations

    def check_conflicts(self) -> set[int]:
        """Checks for conflicts based on Arias default method

        Returns
        -------
        set[int]
            the set of transaction ids to abort
        """
        aborted_transactions = set()
        minimized_writes = self.min_rw_reservations(self.writes)
        for operator_name, write_set in self.write_sets.items():
            read_set = self.read_sets[operator_name]
            t_ids: set[int] = set(read_set.keys()).union(set(write_set.keys()))
            for t_id in t_ids:
                rs = read_set.get(t_id, set())
                ws = write_set.get(t_id, dict())
                read_write_set = rs.union(ws)
                if self.has_conflicts(t_id, read_write_set, minimized_writes[operator_name]):
                    aborted_transactions.add(t_id)
        return aborted_transactions

    def check_conflicts_deterministic_reordering(self) -> set[int]:
        """Checks for conflicts based on Arias deterministic reordering method

        Returns
        -------
        set[int]
            the set of transaction ids to abort
        """
        aborted_transactions = set()
        merged_reads = self.min_rw_reservations(self.__merge_rw_reservations(self.reads, self.remote_reads))
        minimized_writes = self.min_rw_reservations(self.writes)
        merged_write_sets = self.__merge_rw_sets(self.write_sets, self.remote_write_sets)
        merged_read_sets = self.__merge_rw_sets(self.read_sets, self.remote_read_sets)
        for operator_name in self.write_sets.keys():
            write_set = merged_write_sets[operator_name]
            read_set = merged_read_sets[operator_name]
            t_ids: set[int] = set(read_set.keys()).union(set(write_set.keys()))
            for t_id in t_ids:
                ws = write_set.get(t_id, dict())
                waw = self.has_conflicts(t_id, ws, minimized_writes[operator_name])
                if waw:
                    aborted_transactions.add(t_id)
                    continue
                war = self.has_conflicts(t_id, ws, merged_reads[operator_name])
                rs = read_set.get(t_id, set())
                raw = self.has_conflicts(t_id, rs, minimized_writes[operator_name])
                if not war or not raw:
                    continue
                aborted_transactions.add(t_id)
        return aborted_transactions

    def check_conflicts_snapshot_isolation(self) -> set[int]:
        """Checks for conflicts based only on write-after-write dependencies leading to snapshot isolation

        Returns
        -------
        set[int]
            the set of transaction ids to abort
        """
        aborted_transactions = set()
        minimized_writes = self.min_rw_reservations(self.writes)
        for operator_name in self.write_sets.keys():
            t_ids: set[int] = set(self.write_sets[operator_name].keys())
            for t_id in t_ids:
                ws = self.write_sets[operator_name].get(t_id, dict())
                waw = self.has_conflicts(t_id, ws, minimized_writes[operator_name])
                if waw:
                    aborted_transactions.add(t_id)
        return aborted_transactions

    def check_conflicts_serial_reorder_on_in_degree(self) -> set[int]:
        aborted_transactions = set()
        for operator_name in self.operator_names:
            g = get_start_order_serialization_graph(self.read_sets[operator_name], self.write_sets[operator_name])
            aborted_transactions |= check_conflict_on_start_order_serialization_graph(g)
        return aborted_transactions

    def check_conflicts_snapshot_isolation_reorder_on_in_degree(self) -> set[int]:
        aborted_transactions = set()
        for operator_name in self.operator_names:
            g = get_bc_graph(self.read_sets[operator_name], self.write_sets[operator_name])
            aborted_transactions |= check_conflicts_on_bc_graph(g)
        return aborted_transactions

    def cleanup(self):
        self.write_sets = {operator_name: {} for operator_name in self.operator_names}
        self.writes = {operator_name: {} for operator_name in self.operator_names}
        self.reads = {operator_name: {} for operator_name in self.operator_names}
        self.read_sets = {operator_name: {} for operator_name in self.operator_names}
        self.remote_write_sets = {operator_name: {} for operator_name in self.operator_names}
        self.remote_reads = {operator_name: {} for operator_name in self.operator_names}
        self.remote_read_sets = {operator_name: {} for operator_name in self.operator_names}
        self.fallback_commit_buffer = {}

    def merge_rs(self, remote_rs: dict[str, dict[Any, set[Any]]]):
        self.remote_read_sets = self.__merge_rw_sets(self.remote_read_sets, remote_rs)

    def merge_ws(self, remote_ws: dict[str, dict[Any, dict[Any, Any]]]):
        self.remote_write_sets = self.__merge_rw_sets(self.remote_write_sets, remote_ws)

    def merge_rr(self, remote_rr: dict[str, dict[Any, Any]]):
        self.remote_reads = self.__merge_rw_reservations(self.remote_reads, remote_rr)

    @staticmethod
    def __merge_rw_sets(d1: dict[str, dict[Any, set[Any] | dict[Any, Any]]],
                        d2: dict[str, dict[Any, set[Any] | dict[Any, Any]]]
                        ) -> dict[str, dict[Any, set[Any] | dict[Any, Any]]]:
        output_dict: dict[str, dict[Any, set[Any] | dict[Any, Any]]] = {}
        namespaces: set[str] = set(d1.keys()) | set(d2.keys())
        for namespace in namespaces:
            output_dict[namespace] = {}
            if namespace in d1 and namespace in d2:
                t_ids = set(d1[namespace].keys()) | set(d2[namespace].keys())
                for t_id in t_ids:
                    if t_id in d1[namespace] and t_id in d2[namespace]:
                        output_dict[namespace][t_id] = d1[namespace][t_id] | d2[namespace][t_id]
                    elif t_id not in d1[namespace]:
                        output_dict[namespace][t_id] = d2[namespace][t_id]
                    else:
                        output_dict[namespace][t_id] = d1[namespace][t_id]
            elif namespace in d1 and namespace not in d2:
                output_dict[namespace] = d1[namespace]
            elif namespace not in d1 and namespace in d2:
                output_dict[namespace] = d2[namespace]
        return output_dict

    def merge_rw_reservations_fallback(self, c_aborts: set[int]) -> dict[str, dict[any, list[int]]]:
        output_dict: dict[str, dict[any, list[int]]] = {}
        namespaces: set[str] = set(self.reads.keys()) | set(self.writes.keys())
        for namespace in namespaces:
            output_dict[namespace] = {}
            keys = set(self.reads[namespace].keys()) | set(self.writes[namespace].keys())
            for key in keys:
                res = list(set(self.reads[namespace].get(key, [])) & c_aborts |
                           set(self.writes[namespace].get(key, [])) & c_aborts)
                if res:
                    output_dict[namespace][key] = sorted(res)
        return output_dict

    @staticmethod
    def __merge_rw_reservations(d1: dict[str, dict[Any, list[int]]],
                                d2: dict[str, dict[Any, list[int]]]
                                ) -> dict[str, dict[Any, list[int]]]:
        output_dict: dict[str, dict[Any, list[int]]] = {}
        namespaces: set[str] = set(d1.keys()) | set(d2.keys())
        for namespace in namespaces:
            output_dict[namespace] = {}
            if namespace in d1 and namespace in d2:
                keys = set(d1[namespace].keys()) | set(d2[namespace].keys())
                for key in keys:
                    output_dict[namespace][key] = d1[namespace].get(key, []) + d2[namespace].get(key, [])
            elif namespace in d1 and namespace not in d2:
                output_dict[namespace] = d1[namespace]
            elif namespace not in d1 and namespace in d2:
                output_dict[namespace] = d2[namespace]
        return output_dict

    def remove_aborted_from_rw_sets(self, global_logic_aborts: set[int]):
        """
        Here we delete the t_ids of the aborted transactions from the rw sets and reservations as if they never existed
        """
        if len(global_logic_aborts) == 0:
            return
        new_reads = {}
        new_writes = {}
        for operator_name in self.operator_names:
            for aborted_tid in global_logic_aborts:
                if aborted_tid in self.read_sets[operator_name]:
                    del self.read_sets[operator_name][aborted_tid]
                if aborted_tid in self.write_sets[operator_name]:
                    del self.write_sets[operator_name][aborted_tid]
            new_reads[operator_name] = {key: [tid for tid in t_ids if tid not in global_logic_aborts]
                                        for key, t_ids in self.reads[operator_name].items()}
            new_writes[operator_name] = {key: [tid for tid in t_ids if tid not in global_logic_aborts]
                                         for key, t_ids in self.writes[operator_name].items()}
        self.reads = new_reads
        self.writes = new_writes
