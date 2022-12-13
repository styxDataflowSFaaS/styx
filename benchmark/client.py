import asyncio
import time
import pandas as pd
from timeit import default_timer as timer

from kafka.producer.future import FutureRecordMetadata
from styx.common.local_state_backends import LocalStateBackend
from styx.common.operator import Operator
from styx.common.stateflow_graph import StateflowGraph
from styx.common.stateflow_ingress import IngressTypes
from styx.styx import Styx

from benchmark.ycsb import ycsb_operator
from benchmark.zipfian_generator import ZipfGenerator


N_ENTITIES = 10000
# STYX_HOST: str = 'localhost'
# STYX_PORT: int = 8886
STYX_HOST: str = '35.229.80.128'
STYX_PORT: int = 8888
# KAFKA_URL = 'localhost:9093'
KAFKA_URL = '35.229.114.18:9094'


async def submit_graph(styx):
    g = StateflowGraph('ycsb-benchmark', operator_state_backend=LocalStateBackend.DICT)
    ####################################################################################################################
    g.add_operators(ycsb_operator)
    print(list(g.nodes.values())[0].n_partitions)
    await styx.submit(g)
    print("Graph submitted")


async def ycsb_init(styx, operator: Operator, keys: list[int]):
    await submit_graph(styx)
    time.sleep(5)
    # INSERT
    timestamp_futures: dict[int, FutureRecordMetadata] = {}
    for i in keys:
        request_id, metadata = styx.send_kafka_event_no_wait(operator=operator,
                                                             key=i,
                                                             function='insert')
        timestamp_futures[request_id] = metadata
    timestamp_futures = {k: v.get().timestamp for k, v in timestamp_futures.items()}
    return timestamp_futures


def transactional_ycsb_generator(keys, operator: Operator,
                                 n: int, zipf_const: float) -> [Operator, int, str, tuple[int, ]]:
    zipf_gen = ZipfGenerator(items=n, zipf_const=zipf_const)
    uniform_gen = ZipfGenerator(items=n, zipf_const=0.0)
    while True:
        key = keys[next(uniform_gen)]
        key2 = keys[next(zipf_gen)]
        while key2 == key:
            key2 = keys[next(zipf_gen)]
        yield operator, key, 'transfer', (key2, )


def read_only_ycsb_generator(keys, operator: Operator, n: int, zipf_const: float) -> [Operator, int, str, tuple[int, ]]:
    zipf_gen = ZipfGenerator(items=n, zipf_const=zipf_const)
    while True:
        key = keys[next(zipf_gen)]
        yield operator, key, 'read', ()


def benchmark_runner(styx,
                     messages_per_second: int,
                     sleeps_per_second: int,
                     sleep_time: float,
                     seconds: int,
                     workload_generator: []) -> dict[int, FutureRecordMetadata]:
    timestamp_futures: dict[int, FutureRecordMetadata] = {}
    start = timer()
    for _ in range(seconds):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                time.sleep(sleep_time)
            operator, key, func_name, params = next(workload_generator)
            request_id, metadata = styx.send_kafka_event_no_wait(operator=operator,
                                                                 key=key,
                                                                 function=func_name,
                                                                 params=params)
            timestamp_futures[request_id] = metadata
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Latency per second: {sec_end2 - sec_start}')
    end = timer()
    print(f'Average latency per second: {(end - start) / seconds}')
    time.sleep(2)
    timestamp_futures = {k: v.get().timestamp for k, v in timestamp_futures.items()}
    return timestamp_futures


def main():
    key_list: list[int] = list(range(N_ENTITIES))

    styx_client = Styx(STYX_HOST, STYX_PORT, IngressTypes.KAFKA, kafka_url=KAFKA_URL)

    asyncio.run(ycsb_init(styx_client, ycsb_operator, key_list))

    time.sleep(3)

    ycsb_generator = transactional_ycsb_generator(key_list, ycsb_operator, N_ENTITIES, zipf_const=0)

    results = benchmark_runner(styx_client,
                               messages_per_second=5000,
                               sleeps_per_second=100,
                               sleep_time=0.0085,
                               seconds=10,
                               workload_generator=ycsb_generator)

    pd.DataFrame(results.items(), columns=['request_id', 'timestamp']).to_csv(
        'client_requests.csv',
        index=False)


if __name__ == "__main__":
    main()
