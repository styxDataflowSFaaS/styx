import asyncio

from styx.common.networking import NetworkingManager
from styx.common.operator import BaseOperator
from styx.common.stateflow_graph import StateflowGraph
from styx.common.stateflow_worker import StateflowWorker

from .base_scheduler import BaseScheduler


class RoundRobin(BaseScheduler):

    @staticmethod
    async def schedule(workers: dict[int, tuple[str, int]],
                       execution_graph: StateflowGraph,
                       network_manager: NetworkingManager):
        operator_partition_locations: dict[str, dict[str, tuple[str, int]]] = {}
        worker_locations = [StateflowWorker(worker[0], worker[1]) for worker in workers.values()]
        worker_assignments: dict[tuple[str, int], list[tuple[BaseOperator, int]]] = {(worker.host, worker.port): []
                                                                                     for worker in worker_locations}
        for operator_name, operator in iter(execution_graph):
            for partition in range(operator.n_partitions):
                current_worker = worker_locations.pop(0)
                worker_assignments[(current_worker.host, current_worker.port)].append((operator, partition))
                if operator_name in operator_partition_locations:
                    operator_partition_locations[operator_name].update({str(partition): (current_worker.host,
                                                                                         current_worker.port)})
                else:
                    operator_partition_locations[operator_name] = {str(partition): (current_worker.host,
                                                                                    current_worker.port)}
                worker_locations.append(current_worker)

        tasks = [
            asyncio.ensure_future(
                network_manager.send_message(worker[0], 8888,
                                             msg=(operator_partitions,
                                                  operator_partition_locations,
                                                  workers,
                                                  execution_graph.operator_state_backend),
                                             msg_type=4))
            for worker, operator_partitions in worker_assignments.items()]

        await asyncio.gather(*tasks)
        return worker_assignments, operator_partition_locations, execution_graph.operator_state_backend
