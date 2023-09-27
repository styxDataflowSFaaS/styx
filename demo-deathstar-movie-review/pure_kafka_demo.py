import hashlib
import random
import uuid
import json
import sys

from timeit import default_timer as timer
import asyncio
import time
from multiprocessing import Pool

import pandas as pd
from kafka.producer.future import FutureRecordMetadata

from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph
from styx.common.stateflow_ingress import IngressTypes
from styx.styx import Styx

from graph import (user_operator, movie_info_operator, plot_operator, movie_id_operator, compose_review_operator,
                   rating_operator, text_operator, unique_id_operator, frontend_operator)

from workload_data import movie_titles, charset


threads = int(sys.argv[1])
messages_per_second = int(sys.argv[2])
sleeps_per_second = 100
sleep_time = 0.0085
seconds = 60
STYX_HOST: str = 'localhost'
STYX_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


def populate_user(styx_client: Styx):
    timestamp_futures: dict[int, FutureRecordMetadata] = {}
    for i in range(1000):
        user_id = f'user{i}'
        username = f'username_{i}'
        password = f'password_{i}'
        hasher = hashlib.new('sha512')
        salt = uuid.uuid1().bytes
        hasher.update(password.encode())
        hasher.update(salt)

        password_hash = hasher.hexdigest()

        user_data = {
            "userId": user_id,
            "FirstName": "firstname",
            "LastName": "lastname",
            "Username": username,
            "Password": password_hash,
            "Salt": salt
        }
        request_id, metadata = styx_client.send_kafka_event_no_wait(operator=user_operator,
                                                                    key=username,
                                                                    function='register_user',
                                                                    params=(user_data, ))
        timestamp_futures[request_id] = metadata
    timestamp_futures = {k: v.get().timestamp for k, v in timestamp_futures.items()}
    return timestamp_futures


def populate_movie(styx_client: Styx):
    timestamp_futures: dict[int, FutureRecordMetadata] = {}
    with open('data/compressed.json') as json_file:
        movie_data = json.load(json_file)
    for movie in movie_data:
        movie_id = movie["MovieId"]
        request_id, metadata = styx_client.send_kafka_event_no_wait(operator=movie_info_operator,
                                                                    key=movie_id,
                                                                    function='write',
                                                                    params=(movie, ))
        timestamp_futures[request_id] = metadata
        request_id, metadata = styx_client.send_kafka_event_no_wait(operator=plot_operator,
                                                                    key=movie_id,
                                                                    function='write',
                                                                    params=("plot", ))
        timestamp_futures[request_id] = metadata
        request_id, metadata = styx_client.send_kafka_event_no_wait(operator=movie_id_operator,
                                                                    key=movie["Title"],
                                                                    function='register_movie_id',
                                                                    params=(movie_id, ))
        timestamp_futures[request_id] = metadata

    timestamp_futures = {k: v.get().timestamp for k, v in timestamp_futures.items()}
    return timestamp_futures


async def submit_graph(styx):
    g = StateflowGraph('deathstar_movie_review', operator_state_backend=LocalStateBackend.DICT)
    ####################################################################################################################
    g.add_operators(compose_review_operator,
                    movie_id_operator,
                    movie_info_operator,
                    plot_operator,
                    rating_operator,
                    text_operator,
                    unique_id_operator,
                    user_operator,
                    frontend_operator)
    print(list(g.nodes.values())[0].n_partitions)
    await styx.submit(g)
    print("Graph submitted")


async def deathstar_init(styx):
    await submit_graph(styx)
    time.sleep(2)
    populate_user(styx)
    populate_movie(styx)
    print('Data populated')
    time.sleep(2)


def compose_review(c):
    user_index = random.randint(0, 999)
    username = f"username_{user_index}"
    password = f"password_{user_index}"
    title = random.choice(movie_titles)
    rating = random.randint(0, 10)
    text = ''.join(random.choice(charset) for _ in range(256))
    return frontend_operator, c, "compose", (username, title, rating, text)


def deathstar_workload_generator():
    c = 0
    while True:
        yield compose_review(c)
        c += 1


def benchmark_runner(proc_num) -> dict[int, dict]:
    print(f'Generator: {proc_num} starting')
    styx = Styx(STYX_HOST, STYX_PORT, IngressTypes.KAFKA, kafka_url=KAFKA_URL)
    deathstar_generator = deathstar_workload_generator()
    timestamp_futures: dict[int, dict] = {}
    start = timer()
    for _ in range(seconds):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                time.sleep(sleep_time)
            operator, key, func_name, params = next(deathstar_generator)
            request_id, metadata = styx.send_kafka_event_no_wait(operator=operator,
                                                                 key=key,
                                                                 function=func_name,
                                                                 params=params)
            timestamp_futures[request_id] = {"metadata": metadata, "op": f'{func_name} {key}->{params}'}
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Latency per second: {sec_end2 - sec_start}')
    end = timer()
    print(f'Average latency per second: {(end - start) / seconds}')
    time.sleep(2)
    timestamp_futures = {k: {"timestamp": v["metadata"].get().timestamp, "op": v["op"]}
                         for k, v in timestamp_futures.items()}
    return timestamp_futures


def main():
    styx_client = Styx(STYX_HOST, STYX_PORT, IngressTypes.KAFKA, kafka_url=KAFKA_URL)

    asyncio.run(deathstar_init(styx_client))

    with Pool(threads) as p:
        results = p.map(benchmark_runner, range(threads))

    results = {k: v for d in results for k, v in d.items()}
    pd.DataFrame({"request_id": list(results.keys()),
                  "timestamp": [res["timestamp"] for res in results.values()],
                  "op": [res["op"] for res in results.values()]
                  }).to_csv('client_requests.csv',
                            index=False)

    styx_client.sync_kafka_producer.flush()
    styx_client.sync_kafka_producer.close()


if __name__ == "__main__":
    main()
