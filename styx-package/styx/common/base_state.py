from abc import abstractmethod, ABC
from typing import Any


class ReadUncommitedException(Exception):
    pass


class BaseOperatorState(ABC):
    # read write sets
    read_sets: dict[str, dict[int, set[Any]]]  # operator_name: {t_id: set(keys)}
    write_sets: dict[str, dict[int, dict[Any, Any]]]  # operator_name: {t_id: {key: value}}
    # the reads and writes with the lowest t_id
    writes: dict[str, dict[Any, int]]  # operator_name: {key: t_id}
    reads: dict[str, dict[Any, int]]  # operator_name: {key: t_id}
    # the transactions that are aborted
    aborted_transactions: set[int]
    # Calvin snapshot things
    fallback_commit_buffer: dict[int, dict[str, dict[Any, Any]]]  # tid: {operator_name: {key, value}}

    def __init__(self, operator_names: set[str]):
        self.operator_names = operator_names
        self.cleanup()

    async def put(self, key, value, t_id: int, operator_name: str):
        if t_id in self.write_sets[operator_name]:
            self.write_sets[operator_name][t_id][key] = value
        else:
            self.write_sets[operator_name][t_id] = {key: value}
        self.writes[operator_name][key] = min(self.writes[operator_name].get(key, t_id), t_id)

    async def put_immediate(self, key, value, t_id: int, operator_name: str):
        if t_id in self.fallback_commit_buffer:
            if operator_name in self.fallback_commit_buffer[t_id]:
                self.fallback_commit_buffer[t_id][operator_name].update({key: value})
            else:
                self.fallback_commit_buffer[t_id] = {operator_name: {key: value}}
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
        self.reads[operator_name][key] = min(self.reads[operator_name].get(key, t_id), t_id)
        if t_id in self.read_sets[operator_name]:
            self.read_sets[operator_name][t_id].add(key)
        else:
            self.read_sets[operator_name][t_id] = {key}

    @staticmethod
    def t_get_key_set_dependencies(t_id: int,
                                   operator_name: str,
                                   key_set: set[Any],
                                   t_dependencies: dict[int, dict[str, set[Any]]]):
        if t_id in t_dependencies:
            if operator_name in t_dependencies[t_id]:
                t_dependencies[t_id][operator_name].update(t_dependencies[t_id][operator_name].union(key_set))
            else:
                t_dependencies[t_id][operator_name] = key_set
        else:
            t_dependencies[t_id] = {operator_name: key_set}
        return t_dependencies

    def get_dependency_graph(self, aborted_t_ids: set[int],
                             logic_aborts_everywhere: set[int]) -> dict[int, dict[str, set[Any]]]:
        t_dependencies: dict[int, dict[str, set[Any]]] = {}   # tid: {operator_name: {set of keys}}
        # Get write set dependencies
        for operator_name, write_set in self.write_sets.items():
            for t_id, ws in write_set.items():
                if t_id in aborted_t_ids and t_id not in logic_aborts_everywhere:
                    ws_keys: set[Any] = set(ws.keys())
                    t_dependencies = self.t_get_key_set_dependencies(t_id, operator_name, ws_keys, t_dependencies)
        # Get read set dependencies
        for operator_name, read_set in self.read_sets.items():
            for t_id, rs in read_set.items():
                if t_id in aborted_t_ids and t_id not in logic_aborts_everywhere:
                    t_dependencies = self.t_get_key_set_dependencies(t_id, operator_name, rs, t_dependencies)
        t_dependencies = {key: t_dependencies[key] for key in sorted(t_dependencies.keys())}  # sort by t_id
        return t_dependencies

    @staticmethod
    def has_conflicts(t_id: int, keys: set[Any], reservations: dict[Any, int]):
        for key in keys:
            if key in reservations and reservations[key] < t_id:
                return True
        return False

    def check_conflicts(self) -> set[int]:
        self.aborted_transactions = set()
        for operator_name, write_set in self.write_sets.items():
            read_set = self.read_sets[operator_name]
            t_ids: set[int] = set(read_set.keys()).union(set(write_set.keys()))
            for t_id in t_ids:
                rs = read_set.get(t_id, set())
                ws = write_set.get(t_id, set())
                read_write_set = rs.union(ws)
                if self.has_conflicts(t_id, read_write_set, self.writes[operator_name]):
                    self.aborted_transactions.add(t_id)
        return self.aborted_transactions

    def check_conflicts_deterministic_reordering(self) -> set[int]:
        self.aborted_transactions = set()
        for operator_name, write_set in self.write_sets.items():
            read_set = self.read_sets[operator_name]
            t_ids: set[int] = set(read_set.keys()).union(set(write_set.keys()))
            for t_id in t_ids:
                rs = read_set.get(t_id, set())
                ws = write_set.get(t_id, set())
                war = self.has_conflicts(t_id, ws, self.reads[operator_name])
                raw = self.has_conflicts(t_id, rs, self.writes[operator_name])
                if not war or not raw:
                    continue
                self.aborted_transactions.add(t_id)
        return self.aborted_transactions

    def cleanup(self):
        self.write_sets = {operator_name: {} for operator_name in self.operator_names}
        self.writes = {operator_name: {} for operator_name in self.operator_names}
        self.reads = {operator_name: {} for operator_name in self.operator_names}
        self.read_sets = {operator_name: {} for operator_name in self.operator_names}
        self.aborted_transactions = set()
        self.fallback_commit_buffer = {}
